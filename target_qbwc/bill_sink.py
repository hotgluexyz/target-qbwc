"""Bill upsert batch sink with line reconciliation and customData mapping."""

from __future__ import annotations

from typing import Any

from target_qbwc.bill_lines import (
    build_bill_line_custom_data,
    merge_bill_for_mod,
    payload_has_bill_lines,
    preprocess_bill_add_payload,
)
from target_qbwc.client_upsert import QbwcTxnUpsertBatchSink
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

    def _merge_for_mod(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        """Use bill line reconciliation when the payload includes line arrays."""
        if not payload_has_bill_lines(incoming):
            self._bill_line_external_ids = ([], [])
            merged = super()._merge_for_mod(existing, incoming)
        else:
            merged, item_ids, expense_ids = merge_bill_for_mod(
                existing,
                incoming,
                self.qbd_xml_schemas,
                self.id_field,
            )
            self._bill_line_external_ids = (item_ids, expense_ids)
        merged.pop("VendorAddress", None)
        return merged

    def _build_write_request(
        self,
        staged: dict[str, Any],
        query_outcome: dict[str, Any],
    ) -> dict[str, Any]:
        """Preprocess add lines and attach mod line externalId context for customData."""
        self._bill_line_external_ids: tuple[list[str | None], list[str | None]] = (
            [],
            [],
        )
        working = dict(staged)
        matches = self._filter_query_matches(
            query_outcome.get("matches", []),
            staged["payload"],
        )
        if len(matches) == 0:
            payload, item_ids, expense_ids = preprocess_bill_add_payload(working["payload"])
            working["payload"] = payload
            self._bill_line_external_ids = (item_ids, expense_ids)

        write_request = super()._build_write_request(working, query_outcome)
        item_ids, expense_ids = self._bill_line_external_ids
        write_request["bill_item_line_external_ids"] = item_ids
        write_request["bill_expense_line_external_ids"] = expense_ids
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
            staged.get("bill_item_line_external_ids") or [],
            staged.get("bill_expense_line_external_ids") or [],
        )
        if custom_data:
            state["customData"] = custom_data
        return state
