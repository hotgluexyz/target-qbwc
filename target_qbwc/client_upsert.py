"""Query-before-write upsert batch sink for QBXML mod support."""

from __future__ import annotations

from typing import Any

from hotglue_etl_exceptions import InvalidPayloadError

from qbwc_common import filter_dict_for_mod, get_mod_element_names, normalize_rs_list, parse_rs_element

from target_qbwc.client import QbwcBatchSink

LOOKUP_QUERY_FAILED_MESSAGE = "Lookup query failed; not writing to avoid a duplicate"


def _extract_ret_entities(rs_element: dict[str, Any], ret_element_name: str) -> list[dict[str, Any]]:
    """Return all *Ret entities from a query *Rs element."""
    entities = rs_element.get(ret_element_name)
    if entities is None:
        return []
    if isinstance(entities, dict):
        return [entities]
    return list(entities)


def _first_lookup_match(
    payload: dict[str, Any],
    lookup_fields: list[tuple[str, str]],
) -> tuple[str, str, Any] | None:
    """Return the first non-empty payload lookup field and its query element name."""
    for payload_key, query_element in lookup_fields:
        value = payload.get(payload_key)
        if value is not None and value != "":
            return payload_key, query_element, value
    return None


def _format_external_id_log(external_id: str | None) -> str:
    """Format externalId for per-record log lines when present."""
    if external_id:
        return f"externalId: {external_id}, "
    return ""


def filter_matches_by_vendor_ref(
    matches: list[dict[str, Any]],
    payload: dict[str, Any],
    lookup_fields: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Post-filter vendor transaction matches by VendorRef for RefNumber lookups only."""
    lookup_match = _first_lookup_match(payload, lookup_fields)
    if lookup_match is not None and lookup_match[0] == "TxnID":
        return matches

    vendor_ref = payload.get("VendorRef") or {}
    vendor_list_id = vendor_ref.get("ListID")
    vendor_full_name = vendor_ref.get("FullName")
    if not vendor_list_id and not vendor_full_name:
        return matches

    filtered: list[dict[str, Any]] = []
    for entity in matches:
        entity_vendor = entity.get("VendorRef") or {}
        if vendor_list_id and entity_vendor.get("ListID") == vendor_list_id:
            filtered.append(entity)
        elif vendor_full_name and (
            entity_vendor.get("FullName") or ""
        ).lower() == vendor_full_name.lower():
            filtered.append(entity)
    return filtered


class QbwcUpsertBatchSink(QbwcBatchSink):
    """Batch sink with query-before-write upsert support for add and mod operations."""

    lookup_fields: list[tuple[str, str]]
    _batch_item_error_keys = ("ambiguous_error", "lookup_error", "preprocess_error")

    @property
    def query_request_element_name(self) -> str:
        """Return the stream's *QueryRq element name."""
        return f"{self.qbxml_entity}QueryRq"

    @property
    def query_response_element_name(self) -> str:
        """Return the stream's *QueryRs element name."""
        return f"{self.qbxml_entity}QueryRs"

    @property
    def query_ret_element_name(self) -> str:
        """Return the stream's query *Ret element name."""
        return f"{self.qbxml_entity}Ret"

    @property
    def mod_request_element_name(self) -> str:
        """Return the stream's *ModRq element name."""
        return f"{self.qbxml_entity}ModRq"

    @property
    def mod_response_element_name(self) -> str:
        """Return the stream's *ModRs element name."""
        return f"{self.qbxml_entity}ModRs"

    @property
    def entity_mod_name(self) -> str:
        """Return the stream's *Mod entity element name."""
        return f"{self.qbxml_entity}Mod"

    def _resolve_lookup_query_value(
        self,
        payload: dict[str, Any],
        payload_key: str,
        query_element: str,
        value: Any,
    ) -> Any:
        """Return the query value for one lookup field, allowing stream-specific transforms."""
        return value

    def _format_lookup_values(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Build a human-readable lookup dict from the first matching payload field."""
        match = _first_lookup_match(payload, self.lookup_fields)
        if match is None:
            return {}
        payload_key, query_element, value = match
        value = self._resolve_lookup_query_value(payload, payload_key, query_element, value)
        return {query_element: value}

    def process_batch_record(self, record: dict, index: int) -> dict:
        """Strip metadata and stage the payload without add-only XSD validation."""
        payload, external_id = self.strip_hotglue_metadata(record)
        payload = self.build_request_element(payload)
        return {
            "request_id": str(index),
            "external_id": external_id,
            "payload": payload,
        }

    def _build_lookup_query_element(self, payload: dict[str, Any], request_id: str) -> dict[str, Any] | None:
        """Build one *QueryRq element from the first available lookup field."""
        match = _first_lookup_match(payload, self.lookup_fields)
        if match is None:
            return None
        payload_key, query_element, value = match
        value = self._resolve_lookup_query_value(payload, payload_key, query_element, value)
        return {
            self.query_request_element_name: {
                "@requestID": request_id,
                query_element: value,
            }
        }

    def _interpret_query_response(self, rs_element: dict[str, Any] | None) -> dict[str, Any]:
        """Parse a query *Rs element into match list and failure flag."""
        if rs_element is None:
            return {"matches": [], "query_failed": True}

        raw_status = rs_element.get("@statusCode")
        status_code = int(raw_status) if raw_status is not None else -1
        status_message = str(rs_element.get("@statusMessage", ""))

        if status_code == 0:
            matches = _extract_ret_entities(rs_element, self.query_ret_element_name)
            return {"matches": matches, "query_failed": False}

        if status_code == 500 and "could not be found" in status_message.lower():
            return {"matches": [], "query_failed": False}

        return {"matches": [], "query_failed": True}

    def _merge_for_mod(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        """Overlay incoming fields onto the queried record for a mod request.

        Query responses return fat *Ret objects. Filter existing to the *Mod XSD
        allowlist so Ret-only keys (balances, timestamps, line arrays) are not
        carried into the mod payload. Incoming user payload is not filtered here:
        unknown or Ret-shaped keys must fail encode validation in _build_write_request
        rather than be silently dropped.
        """
        allowed = get_mod_element_names(self.qbd_xml_schemas, self.entity_mod_name)
        merged = filter_dict_for_mod(existing, allowed)
        overlay = dict(incoming)
        overlay.pop("ListID", None)
        overlay.pop("TxnID", None)
        merged.update(overlay)
        merged[self.id_field] = existing[self.id_field]
        merged["EditSequence"] = existing["EditSequence"]
        return merged

    def _filter_query_matches(
        self,
        matches: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Allow sinks to post-filter lookup matches before add/mod selection."""
        return matches

    def _build_ambiguous_match_error(self, payload: dict[str, Any]) -> InvalidPayloadError:
        """Build the error for multiple lookup matches."""
        lookup = self._format_lookup_values(payload)
        return InvalidPayloadError(
            "Unable to create or update record, as there are multiple existing records "
            f"in Quickbooks with the same identifiers {lookup}"
        )

    def _log_write_decision(
        self,
        staged: dict[str, Any],
        write_op: str,
        existing_id: str | None = None,
    ) -> None:
        """Log the add vs mod decision for one staged record."""
        payload = staged["payload"]
        external_id_log = _format_external_id_log(staged.get("external_id"))
        lookup_match = _first_lookup_match(payload, self.lookup_fields)

        if write_op == "mod":
            lookup_fragment = ""
            if lookup_match is not None:
                payload_key, query_element, value = lookup_match
                value = self._resolve_lookup_query_value(payload, payload_key, query_element, value)
                lookup_fragment = f"lookup matched {query_element}={value}, "
            self.logger.info(
                "%s %sid: %s, %sop: mod",
                self.name,
                lookup_fragment,
                existing_id,
                external_id_log,
            )
            return

        if lookup_match is None:
            self.logger.info(
                "%s no lookup field, %sop: add",
                self.name,
                external_id_log,
            )
            return

        payload_key, query_element, value = lookup_match
        value = self._resolve_lookup_query_value(payload, payload_key, query_element, value)
        self.logger.info(
            "%s lookup no match %s=%s, %sop: add",
            self.name,
            query_element,
            value,
            external_id_log,
        )

    def _build_write_request(
        self,
        staged: dict[str, Any],
        query_outcome: dict[str, Any],
    ) -> dict[str, Any]:
        """Choose add or mod and build the write request element for one staged record."""
        matches = self._filter_query_matches(
            query_outcome.get("matches", []),
            staged["payload"],
        )
        if query_outcome.get("query_failed"):
            error = query_outcome.get("lookup_error") or Exception(
                LOOKUP_QUERY_FAILED_MESSAGE
            )
            return {"lookup_error": error}

        if len(matches) > 1:
            return {"ambiguous_error": self._build_ambiguous_match_error(staged["payload"])}

        if len(matches) == 1:
            existing = matches[0]
            self._log_write_decision(staged, "mod", existing.get(self.id_field))
            mod_payload = self._merge_for_mod(existing, staged["payload"])
            request_element = {
                self.mod_request_element_name: {
                    "@requestID": staged["request_id"],
                    self.entity_mod_name: mod_payload,
                }
            }
            return {
                "request_element": request_element,
                "write_op": "mod",
                "write_response_element": self.mod_response_element_name,
                "preprocess_error": self._validate_request_element(request_element),
            }

        self._log_write_decision(staged, "add")
        request_element = self._build_request_element(staged["payload"], int(staged["request_id"]))
        return {
            "request_element": request_element,
            "write_op": "add",
            "write_response_element": self.response_element_name,
            "preprocess_error": self._validate_request_element(request_element),
        }

    def _execute_lookup_queries(self, staged_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run batched lookup queries and return per-record query outcomes."""
        outcomes: list[dict[str, Any] | None] = [None] * len(staged_records)
        query_elements: list[dict[str, Any]] = []
        query_indices: list[int] = []

        for index, staged in enumerate(staged_records):
            query_element = self._build_lookup_query_element(staged["payload"], staged["request_id"])
            if query_element is None:
                outcomes[index] = {"matches": [], "query_failed": False}
            else:
                query_elements.append(query_element)
                query_indices.append(index)

        if not query_elements:
            return [outcome or {"matches": [], "query_failed": False} for outcome in outcomes]

        try:
            qbxml_msgs_rs = self.send_qbxml_batch(query_elements)
            responses = normalize_rs_list(qbxml_msgs_rs, self.query_response_element_name)
            responses_by_id = {response.get("@requestID", ""): response for response in responses}
        except Exception as exc:
            mapped = self.map_qbwc_error(exc)
            self.logger.warning(
                "Lookup query batch failed for %s, skipping write for %d record(s): %s",
                self.name,
                len(query_indices),
                mapped,
            )
            for index in query_indices:
                outcomes[index] = {
                    "matches": [],
                    "query_failed": True,
                    "lookup_error": mapped,
                }
            return [outcome or {"matches": [], "query_failed": False} for outcome in outcomes]

        for query_element, index in zip(query_elements, query_indices):
            request_id = query_element[self.query_request_element_name]["@requestID"]
            rs_element = responses_by_id.get(request_id)
            outcome = self._interpret_query_response(rs_element)
            if outcome["query_failed"]:
                self.logger.warning(
                    "Lookup query failed for %s record request_id=%s, skipping write",
                    self.name,
                    request_id,
                )
            outcomes[index] = outcome

        return [outcome or {"matches": [], "query_failed": False} for outcome in outcomes]

    def _execute_write_batch(self, write_staged: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Send one batched write message and pair each *Rs with its staged record."""
        request_elements = [staged["request_element"] for staged in write_staged]
        qbxml_msgs_rs = self.send_qbxml_batch(request_elements)

        response_element_names = {
            staged["write_response_element"] for staged in write_staged
        }
        responses_by_id: dict[str, dict[str, Any]] = {}
        for response_element_name in response_element_names:
            for response in normalize_rs_list(qbxml_msgs_rs, response_element_name):
                responses_by_id[response.get("@requestID", "")] = response

        items: list[dict[str, Any]] = []
        for staged in write_staged:
            parsed = None
            raw_response = responses_by_id.get(staged["request_id"])
            if raw_response is not None:
                parsed = parse_rs_element(raw_response)
            items.append(
                {
                    "record": staged,
                    "response": parsed,
                }
            )
        return items

    def make_batch_request(self, records: list[dict]) -> dict:
        """Run batched lookup queries then a batched add/mod write for the batch."""
        items_by_request_id: dict[str, dict[str, Any]] = {}
        valid_records = [record for record in records if "preprocess_error" not in record]

        for staged in records:
            if "preprocess_error" in staged:
                items_by_request_id[staged["request_id"]] = {
                    "record": staged,
                    "preprocess_error": staged["preprocess_error"],
                }

        if not valid_records:
            return {
                "items": [items_by_request_id[staged["request_id"]] for staged in records]
            }

        self.logger.info("%s batch lookup: %d record(s)", self.name, len(valid_records))
        query_outcomes = self._execute_lookup_queries(valid_records)
        write_staged: list[dict[str, Any]] = []

        for staged, query_outcome in zip(valid_records, query_outcomes):
            write_request = self._build_write_request(staged, query_outcome)

            ambiguous_error = write_request.get("ambiguous_error")
            if ambiguous_error is not None:
                items_by_request_id[staged["request_id"]] = {
                    "record": staged,
                    "ambiguous_error": ambiguous_error,
                }
                continue

            lookup_error = write_request.get("lookup_error")
            if lookup_error is not None:
                items_by_request_id[staged["request_id"]] = {
                    "record": staged,
                    "lookup_error": lookup_error,
                }
                continue

            preprocess_error = write_request.get("preprocess_error")
            if preprocess_error is not None:
                items_by_request_id[staged["request_id"]] = {
                    "record": staged,
                    "preprocess_error": preprocess_error,
                }
                continue

            write_staged.append({**staged, **write_request})

        if write_staged:
            add_count = sum(1 for staged in write_staged if staged.get("write_op") == "add")
            mod_count = sum(1 for staged in write_staged if staged.get("write_op") == "mod")
            self.logger.info(
                "%s batch write: %d record(s) (%d add, %d mod)",
                self.name,
                len(write_staged),
                add_count,
                mod_count,
            )
            for item in self._execute_write_batch(write_staged):
                staged = item["record"]
                items_by_request_id[staged["request_id"]] = item

        return {
            "items": [items_by_request_id[staged["request_id"]] for staged in records]
        }

    def _enrich_success_state(
        self,
        state: dict[str, Any],
        staged: dict[str, Any],
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        """Mark successful mod writes so the SDK increments summary.updated."""
        if staged.get("write_op") == "mod":
            state["is_updated"] = True
        return state


class QbwcListUpsertBatchSink(QbwcUpsertBatchSink):
    """Upsert sink for list entities looked up by ListID or Name."""

    id_field = "ListID"
    lookup_fields = [("ListID", "ListID"), ("Name", "FullName")]


class QbwcTxnUpsertBatchSink(QbwcUpsertBatchSink):
    """Upsert sink for transaction entities looked up by TxnID or RefNumber."""

    id_field = "TxnID"
    lookup_fields = [("TxnID", "TxnID"), ("RefNumber", "RefNumber")]
