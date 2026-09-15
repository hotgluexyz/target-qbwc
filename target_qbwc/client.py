"""Shared QBWC transport and batch sink base classes."""

from __future__ import annotations

from typing import Any

from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink
from pydantic import BaseModel
from requests.exceptions import RequestException

from qbwc_common import (
    ON_ERROR_CONTINUE,
    QBWCAuthenticationError,
    QBWCClient,
    QBWCEnqueueError,
    QBWCNotAuthenticatedError,
    QBWCQueueFullError,
    QBWCRequestError,
    QBWCRequestTimeoutError,
    QBWCUnknownPollStatusError,
    QBXMLDecodeError,
    QBXMLEncodeError,
    encode_requests,
    load_qbd_xml_schemas,
    normalize_rs_list,
    parse_rs_element,
)

DEFAULT_BATCH_SIZE = 100
WRITE_DECODE_VALIDATION = "skip"


def _format_record_identifiers(identifiers: dict[str, Any]) -> str:
    """Format id and externalId for log lines."""
    parts = []
    if identifiers.get("externalId"):
        parts.append(f"externalId={identifiers['externalId']}")
    if identifiers.get("id"):
        parts.append(f"id={identifiers['id']}")
    return f" ({', '.join(parts)})" if parts else ""


class QbwcTransportMixin:
    """Shared QBWC client, config and transport error mapping."""

    @property
    def unified_schema(self) -> type[BaseModel] | None:
        """QuickBooks-shaped input is validated by the XSD, not a unified model."""
        return None

    @property
    def qbd_xml_schemas(self):
        """Return the bundled qbXML schema set (cached in qbwc-common)."""
        return load_qbd_xml_schemas()

    @property
    def qbwc_client(self) -> QBWCClient:
        """Return the target's shared authenticated QBWC client."""
        return self._target.qbwc_client

    def map_qbwc_error(self, error: Exception) -> Exception:
        """Map qbwc-common transport errors onto SDK and Hotglue exceptions."""
        if isinstance(
            error,
            (InvalidCredentialsError, InvalidPayloadError, RetriableAPIError, FatalAPIError),
        ):
            return error
        if isinstance(error, (QBWCAuthenticationError, QBWCNotAuthenticatedError)):
            return InvalidCredentialsError(str(error))
        if isinstance(
            error,
            (QBWCQueueFullError, QBWCRequestTimeoutError, RequestException),
        ):
            return RetriableAPIError(str(error))
        if isinstance(
            error,
            (
                QBWCEnqueueError,
                QBWCRequestError,
                QBWCUnknownPollStatusError,
                QBXMLDecodeError,
                QBXMLEncodeError,
            ),
        ):
            return FatalAPIError(str(error))
        return FatalAPIError(str(error))

    def send_qbxml_batch(self, request_elements: list[dict[str, Any]]) -> dict[str, Any]:
        """Enqueue one QBXML message and return the decoded QBXMLMsgsRs body."""
        try:
            return self.qbwc_client.make_request(
                request_elements,
                on_error=ON_ERROR_CONTINUE,
                decode_validation=WRITE_DECODE_VALIDATION,
            )
        except Exception as exc:
            raise self.map_qbwc_error(exc) from exc


class QbwcBatchSink(QbwcTransportMixin, HotglueBatchSink):
    """Base batch sink for QBXML write streams."""

    qbxml_entity: str
    id_field: str = "ListID"

    @property
    def request_element_name(self) -> str:
        """Return the stream's *AddRq element name."""
        return f"{self.qbxml_entity}AddRq"

    @property
    def response_element_name(self) -> str:
        """Return the stream's *AddRs element name."""
        return f"{self.qbxml_entity}AddRs"

    @property
    def entity_add_name(self) -> str:
        """Return the stream's *Add entity element name."""
        return f"{self.qbxml_entity}Add"

    @property
    def max_size(self) -> int:
        """Cap records per QBXML message from target config."""
        return int(self.config.get("batch_size", DEFAULT_BATCH_SIZE))

    def build_request_element(self, record: dict) -> dict:
        """Build the inner entity payload for the stream's add request."""
        return record

    def strip_hotglue_metadata(self, record: dict) -> tuple[dict, str | None]:
        """Remove Hotglue fields before QBXML encoding."""
        payload = dict(record)
        external_id_key = self._target.EXTERNAL_ID_KEY
        external_id = payload.pop(external_id_key, None) or payload.pop(
            external_id_key.lower(), None
        )
        return payload, external_id

    def _build_request_element(self, payload: dict, index: int) -> dict[str, Any]:
        """Wrap a payload in the stream's *AddRq element with a requestID."""
        return {
            self.request_element_name: {
                "@requestID": str(index),
                self.entity_add_name: payload,
            }
        }

    def _validate_request_element(self, request_element: dict[str, Any]) -> InvalidPayloadError | None:
        """Return InvalidPayloadError when the XSD rejects a staged record."""
        try:
            encode_requests(
                request_element,
                self.qbd_xml_schemas,
                on_error=ON_ERROR_CONTINUE,
            )
        except QBXMLEncodeError as exc:
            return InvalidPayloadError(str(exc))
        return None

    def process_error_state(self, state: dict) -> dict:
        """Log a single-line record failure."""
        message = state.get("error")
        identifiers = _format_record_identifiers(state)
        hg_error_class = state.get("hg_error_class")
        if hg_error_class in {
            InvalidCredentialsError.__name__,
            InvalidPayloadError.__name__,
        }:
            self.logger.warning(
                "Error processing record of type %s%s: %s",
                self.name,
                identifiers,
                message,
            )
        else:
            self.logger.error(
                "Error processing record of type %s%s: %s",
                self.name,
                identifiers,
                message,
            )
        state["error"] = message
        return state

    def update_state(self, state: dict, is_duplicate: bool = False, record: dict | None = None):
        """Log per-record outcomes; batch sinks do not get this from the SDK by default."""
        if state.pop("is_existing", False):
            is_duplicate = True
        if state.get("success") and not is_duplicate:
            parts = []
            if state.get("id"):
                parts.append(f"id: {state['id']}")
            if state.get("externalId"):
                parts.append(f"externalId: {state['externalId']}")
            detail = ", ".join(parts) or "record"
            if state.get("is_updated"):
                self.logger.info("%s updated %s", self.name, detail)
            else:
                self.logger.info("%s created %s", self.name, detail)
        super().update_state(state, is_duplicate=is_duplicate, record=record)

    def process_batch_record(self, record: dict, index: int) -> dict:
        """Strip metadata, build the QBXML request element and stage state context."""
        payload, external_id = self.strip_hotglue_metadata(record)
        payload = self.build_request_element(payload)
        request_element = self._build_request_element(payload, index)
        preprocess_error = self._validate_request_element(request_element)
        staged = {
            "request_id": str(index),
            "external_id": external_id,
            "payload": payload,
            "request_element": request_element,
        }
        if preprocess_error is not None:
            staged["preprocess_error"] = preprocess_error
        return staged

    _batch_item_error_keys: tuple[str, ...] = ("preprocess_error",)

    def _batch_item_context(
        self, item: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any], str, str | None]:
        """Return staged record, payload, hash, and external_id from one batch item."""
        staged = item["record"]
        payload = staged["payload"]
        return staged, payload, self.build_record_hash(payload), staged.get("external_id")

    def _build_batch_item_error_state(self, item: dict[str, Any]) -> dict[str, Any] | None:
        """Return per-record error state for known preprocessing failures."""
        _, payload, record_hash, external_id = self._batch_item_context(item)
        for key in self._batch_item_error_keys:
            error = item.get(key)
            if error is not None:
                return self._build_record_error_state(
                    error,
                    record=payload,
                    external_id=external_id,
                    record_hash=record_hash,
                )
        return None

    def _build_missing_response_state(
        self,
        staged: dict[str, Any],
        payload: dict[str, Any],
        record_hash: str,
        external_id: str | None,
    ) -> dict[str, Any]:
        """Return failure state when QuickBooks did not return a matching *Rs."""
        return self._build_record_error_state(
            InvalidPayloadError(
                "No matching QuickBooks response for request "
                f"{staged['request_id']}"
            ),
            record=payload,
            external_id=external_id,
            record_hash=record_hash,
        )

    def _build_success_state(
        self,
        parsed: dict[str, Any],
        record_hash: str,
        external_id: str | None,
    ) -> dict[str, Any]:
        """Return success state from a parsed QuickBooks *Rs element."""
        entity = parsed.get("entity") or {}
        state: dict[str, Any] = {
            "success": True,
            "hash": record_hash,
            "id": entity.get(self.id_field),
        }
        if external_id:
            state["externalId"] = str(external_id)
        return state

    def _build_quickbooks_rejection_state(
        self,
        parsed: dict[str, Any],
        payload: dict[str, Any],
        record_hash: str,
        external_id: str | None,
    ) -> dict[str, Any]:
        """Return failure state for a non-zero QuickBooks status code."""
        raw_status = parsed.get("status_code")
        status_code = int(raw_status) if raw_status is not None else -1
        message = f"[{status_code}] {parsed.get('status_message', '')}".strip()
        return self._build_record_error_state(
            InvalidPayloadError(message),
            record=payload,
            external_id=external_id,
            record_hash=record_hash,
        )

    def _enrich_success_state(
        self,
        state: dict[str, Any],
        staged: dict[str, Any],
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        """Hook for subclasses to add fields to a successful batch item state."""
        return state

    def _build_existing_skip_state(
        self,
        record_hash: str,
        external_id: str | None,
        existing_id: str | None,
    ) -> dict[str, Any]:
        """Return success state for a record skipped because the entity already exists."""
        state: dict[str, Any] = {
            "success": True,
            "hash": record_hash,
            "is_existing": True,
        }
        if existing_id:
            state["id"] = existing_id
        if external_id:
            state["externalId"] = str(external_id)
        return state

    def make_batch_request(self, records: list[dict]) -> dict:
        """Send one batched QBXML message and pair each *Rs with its staged record."""
        valid_records = [record for record in records if "preprocess_error" not in record]
        responses_by_id: dict[str, dict[str, Any] | None] = {}

        if valid_records:
            self.logger.info(
                "%s batch write: %d record(s) (add only)",
                self.name,
                len(valid_records),
            )
            request_elements = [record["request_element"] for record in valid_records]
            qbxml_msgs_rs = self.send_qbxml_batch(request_elements)
            responses = normalize_rs_list(qbxml_msgs_rs, self.response_element_name)
            responses_by_id = {
                parsed["request_id"]: parsed
                for response in responses
                for parsed in [parse_rs_element(response)]
            }

        items: list[dict[str, Any]] = []
        for staged in records:
            if "preprocess_error" in staged:
                items.append(
                    {
                        "record": staged,
                        "preprocess_error": staged["preprocess_error"],
                    }
                )
            else:
                items.append(
                    {
                        "record": staged,
                        "response": responses_by_id.get(staged["request_id"]),
                    }
                )

        return {"items": items}

    def handle_batch_response(self, result: dict) -> dict:
        """Convert paired batch results into per-record hotglue state updates."""
        state_updates = []
        for item in result.get("items", []):
            if item.get("existing_skip"):
                _, _, record_hash, external_id = self._batch_item_context(item)
                state_updates.append(
                    self._build_existing_skip_state(
                        record_hash,
                        external_id,
                        item.get("existing_id") or item["record"].get("resolved_entity_id"),
                    )
                )
                continue

            error_state = self._build_batch_item_error_state(item)
            if error_state is not None:
                state_updates.append(error_state)
                continue

            staged, payload, record_hash, external_id = self._batch_item_context(item)
            parsed = item.get("response")

            if not parsed:
                state_updates.append(
                    self._build_missing_response_state(
                        staged, payload, record_hash, external_id
                    )
                )
                continue

            raw_status = parsed.get("status_code")
            status_code = int(raw_status) if raw_status is not None else -1
            if status_code == 0:
                state = self._build_success_state(parsed, record_hash, external_id)
                state_updates.append(
                    self._enrich_success_state(state, staged, parsed)
                )
                continue

            state_updates.append(
                self._build_quickbooks_rejection_state(
                    parsed, payload, record_hash, external_id
                )
            )
        return {"state_updates": state_updates}
