"""Bill upsert batch sink with line reconciliation and customData mapping."""

from __future__ import annotations

from typing import Any

from target_qbwc.bill_lines import (
    EMPTY_BILL_LINE_MAPPING,
    BillLineMapping,
    build_bill_line_custom_data,
    merge_bill_for_mod,
    payload_has_bill_lines,
    preprocess_bill_add_payload,
)
from target_qbwc.client_upsert import QbwcTxnUpsertBatchSink, filter_matches_by_vendor_ref
from target_qbwc.uom_sink import QbwcUomTxnMixin


class QbwcBillUpsertBatchSink(QbwcUomTxnMixin, QbwcTxnUpsertBatchSink):
    """Upsert sink for bills with line reconciliation and line customData mapping."""

    def _build_lookup_query_element(
        self,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any] | None:
        """Request line items on bill lookup so mod reconciliation has *LineRet data."""
        query_element = super()._build_lookup_query_element(payload, request_id)
        if query_element is None:
            return None
        query_element[self.query_request_element_name]["IncludeLineItems"] = "true"
        return query_element

    def _filter_query_matches(
        self,
        matches: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Post-filter bill query matches by VendorRef when RefNumber lookup is used."""
        return filter_matches_by_vendor_ref(matches, payload, self.lookup_fields)

    def _merge_for_mod(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        """Use bill line reconciliation when the payload includes line arrays."""
        if not payload_has_bill_lines(incoming):
            self._bill_line_mappings = (
                EMPTY_BILL_LINE_MAPPING,
                EMPTY_BILL_LINE_MAPPING,
            )
            merged = super()._merge_for_mod(existing, incoming)
        else:
            merged, item_mapping, expense_mapping = merge_bill_for_mod(
                existing,
                incoming,
                self.qbd_xml_schemas,
                self.id_field,
            )
            self._bill_line_mappings = (item_mapping, expense_mapping)
        merged.pop("VendorAddress", None)
        return merged

    def _build_write_request(
        self,
        staged: dict[str, Any],
        query_outcome: dict[str, Any],
    ) -> dict[str, Any]:
        """Preprocess add lines and attach mod line mapping context for customData."""
        self._bill_line_mappings: tuple[BillLineMapping, BillLineMapping] = (
            EMPTY_BILL_LINE_MAPPING,
            EMPTY_BILL_LINE_MAPPING,
        )
        working = dict(staged)
        if query_outcome.get("query_failed"):
            write_request = super()._build_write_request(working, query_outcome)
            write_request["bill_item_line_mapping"] = EMPTY_BILL_LINE_MAPPING
            write_request["bill_expense_line_mapping"] = EMPTY_BILL_LINE_MAPPING
            return write_request

        matches = self._filter_query_matches(
            query_outcome.get("matches", []),
            staged["payload"],
        )
        if len(matches) == 0:
            payload, item_mapping, expense_mapping = preprocess_bill_add_payload(
                working["payload"]
            )
            working["payload"] = payload
            self._bill_line_mappings = (item_mapping, expense_mapping)

        write_request = super()._build_write_request(
            working,
            {**query_outcome, "matches": matches},
        )
        item_mapping, expense_mapping = self._bill_line_mappings
        write_request["bill_item_line_mapping"] = item_mapping
        write_request["bill_expense_line_mapping"] = expense_mapping
        return write_request

    def _enrich_success_state(
        self,
        state: dict[str, Any],
        staged: dict[str, Any],
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        """Add bill line customData and mark successful mod writes as updated."""
        state = super()._enrich_success_state(state, staged, parsed)
        custom_data = build_bill_line_custom_data(
            parsed.get("entity") or {},
            staged.get("bill_item_line_mapping") or EMPTY_BILL_LINE_MAPPING,
            staged.get("bill_expense_line_mapping") or EMPTY_BILL_LINE_MAPPING,
        )
        if custom_data:
            state["customData"] = custom_data
        return state
