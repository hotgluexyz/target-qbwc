"""Query-before-write upsert batch sink for QBXML mod support."""

from __future__ import annotations

from typing import Any

from hotglue_etl_exceptions import InvalidPayloadError

from qbwc_common import normalize_rs_list, parse_rs_element

from target_qbwc.client import QbwcBatchSink

# *Ret fields that are not valid on *Mod payloads and should be stripped before merge.
QUERY_ONLY_RET_FIELDS = frozenset(
    {
        "TimeCreated",
        "TimeModified",
        "FullName",
        "Sublevel",
        "Balance",
        "TotalBalance",
        "ExternalGUID",
        "BillAddressBlock",
        "ShipAddressBlock",
        "ContactsRet",
        "AdditionalNotesRet",
        "DataExtRet",
    }
)


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


def _format_lookup_values(payload: dict[str, Any], lookup_fields: list[tuple[str, str]]) -> dict[str, Any]:
    """Build a human-readable lookup dict from the first matching payload field."""
    match = _first_lookup_match(payload, lookup_fields)
    if match is None:
        return {}
    _, query_element, value = match
    return {query_element: value}


class QbwcUpsertBatchSink(QbwcBatchSink):
    """Batch sink with query-before-write upsert support for add and mod operations."""

    lookup_fields: list[tuple[str, str]]
    query_only_ret_fields: frozenset[str] = QUERY_ONLY_RET_FIELDS
    _batch_item_error_keys = ("ambiguous_error", "preprocess_error")

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

    def process_batch_record(self, record: dict, index: int) -> dict:
        """Strip metadata and validate the payload before lookup and write staging."""
        staged = super().process_batch_record(record, index)
        staged.pop("request_element", None)
        return staged

    def _build_lookup_query_element(self, payload: dict[str, Any], request_id: str) -> dict[str, Any] | None:
        """Build one *QueryRq element from the first available lookup field."""
        match = _first_lookup_match(payload, self.lookup_fields)
        if match is None:
            return None
        _, query_element, value = match
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

    def _strip_ret_for_mod(self, ret_entity: dict[str, Any]) -> dict[str, Any]:
        """Remove query-only *Ret fields before building a mod payload."""
        return {
            key: value
            for key, value in ret_entity.items()
            if key not in self.query_only_ret_fields
        }

    def _merge_for_mod(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        """Overlay incoming fields onto the queried record for a mod request."""
        merged = self._strip_ret_for_mod(dict(existing))
        overlay = dict(incoming)
        overlay.pop("ListID", None)
        overlay.pop("TxnID", None)
        merged.update(overlay)
        merged["ListID"] = existing["ListID"]
        merged["EditSequence"] = existing["EditSequence"]
        return merged

    def _build_ambiguous_match_error(self, payload: dict[str, Any]) -> InvalidPayloadError:
        """Build the legacy-style error for multiple lookup matches."""
        lookup = _format_lookup_values(payload, self.lookup_fields)
        return InvalidPayloadError(
            "Unable to create or update record, as there are multiple existing records "
            f"in Quickbooks with the same identifiers {lookup}"
        )

    def _build_write_request(
        self,
        staged: dict[str, Any],
        query_outcome: dict[str, Any],
    ) -> dict[str, Any]:
        """Choose add or mod and build the write request element for one staged record."""
        matches = query_outcome.get("matches", [])
        if len(matches) > 1:
            return {"ambiguous_error": self._build_ambiguous_match_error(staged["payload"])}

        if len(matches) == 1:
            mod_payload = self._merge_for_mod(matches[0], staged["payload"])
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
                "Lookup query batch failed for %s, proceeding as add for %d record(s): %s",
                self.name,
                len(query_indices),
                mapped,
            )
            for index in query_indices:
                outcomes[index] = {"matches": [], "query_failed": True}
            return [outcome or {"matches": [], "query_failed": False} for outcome in outcomes]

        for query_element, index in zip(query_elements, query_indices):
            request_id = query_element[self.query_request_element_name]["@requestID"]
            rs_element = responses_by_id.get(request_id)
            outcome = self._interpret_query_response(rs_element)
            if outcome["query_failed"]:
                self.logger.warning(
                    "Lookup query failed for %s record request_id=%s, proceeding as add",
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
        items: list[dict[str, Any]] = []
        valid_records = [record for record in records if "preprocess_error" not in record]

        for staged in records:
            if "preprocess_error" in staged:
                items.append(
                    {
                        "record": staged,
                        "preprocess_error": staged["preprocess_error"],
                    }
                )

        if not valid_records:
            return {"items": items}

        query_outcomes = self._execute_lookup_queries(valid_records)
        write_staged: list[dict[str, Any]] = []

        for staged, query_outcome in zip(valid_records, query_outcomes):
            write_request = self._build_write_request(staged, query_outcome)

            ambiguous_error = write_request.get("ambiguous_error")
            if ambiguous_error is not None:
                items.append(
                    {
                        "record": staged,
                        "ambiguous_error": ambiguous_error,
                    }
                )
                continue

            preprocess_error = write_request.get("preprocess_error")
            if preprocess_error is not None:
                items.append(
                    {
                        "record": staged,
                        "preprocess_error": preprocess_error,
                    }
                )
                continue

            write_staged.append({**staged, **write_request})

        if write_staged:
            items.extend(self._execute_write_batch(write_staged))

        return {"items": items}

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
