"""QBWC target sink classes."""

from __future__ import annotations

from typing import Any

from qbwc_common import normalize_rs_list

from target_qbwc.bill_sink import QbwcBillUpsertBatchSink
from target_qbwc.customer_lookup import customer_full_name_for_lookup, parent_list_id_for_lookup
from target_qbwc.uom_sink import QbwcUomTxnMixin
from target_qbwc.client_upsert import (
    QbwcItemUpsertBatchSink,
    QbwcListUpsertBatchSink,
    QbwcTxnUpsertBatchSink,
    filter_matches_by_vendor_ref,
)


class CustomersSink(QbwcListUpsertBatchSink):
    """Writes customer records to QuickBooks Desktop."""

    name = "customer"
    qbxml_entity = "Customer"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._parent_full_name_cache: dict[str, str | None] = {}

    def _execute_lookup_queries(self, staged_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Prefetch parent FullNames, then run the batched customer lookup queries."""
        self._prefetch_parent_full_names(staged_records)
        return super()._execute_lookup_queries(staged_records)

    def _prefetch_parent_full_names(self, staged_records: list[dict[str, Any]]) -> None:
        """Batch-query unique parent ListIDs needed for sub-customer FullName lookup."""
        self._parent_full_name_cache = {}
        parent_list_ids: list[str] = []
        seen: set[str] = set()

        for staged in staged_records:
            parent_list_id = parent_list_id_for_lookup(staged["payload"])
            if parent_list_id and parent_list_id not in seen:
                seen.add(parent_list_id)
                parent_list_ids.append(parent_list_id)

        if not parent_list_ids:
            return

        query_elements = [
            {
                "CustomerQueryRq": {
                    "@requestID": f"parent-{parent_list_id}",
                    "ListID": parent_list_id,
                }
            }
            for parent_list_id in parent_list_ids
        ]

        try:
            qbxml_msgs_rs = self.send_qbxml_batch(query_elements)
            responses = normalize_rs_list(qbxml_msgs_rs, "CustomerQueryRs")
            responses_by_id = {response.get("@requestID", ""): response for response in responses}
            for parent_list_id in parent_list_ids:
                request_id = f"parent-{parent_list_id}"
                outcome = self._interpret_query_response(responses_by_id.get(request_id))
                if outcome.get("query_failed") or not outcome.get("matches"):
                    self._parent_full_name_cache[parent_list_id] = None
                else:
                    self._parent_full_name_cache[parent_list_id] = outcome["matches"][0].get("FullName")
        except Exception:
            self.logger.exception(
                "Failed to batch query customer parents for ParentRef.ListID lookup",
            )
            for parent_list_id in parent_list_ids:
                self._parent_full_name_cache[parent_list_id] = None

    def _resolve_lookup_query_value(
        self,
        payload: dict[str, Any],
        payload_key: str,
        query_element: str,
        value: Any,
    ) -> Any:
        """Stitch ParentRef into FullName when looking up sub-customers by Name."""
        if payload_key == "Name" and query_element == "FullName":
            return customer_full_name_for_lookup(
                payload,
                parent_full_name_resolver=self._query_customer_full_name_by_list_id,
            )
        return value

    def _query_customer_full_name_by_list_id(self, parent_list_id: str) -> str | None:
        """Return a prefetched parent FullName for ParentRef.ListID lookup."""
        return self._parent_full_name_cache.get(parent_list_id)


class VendorsSink(QbwcListUpsertBatchSink):
    """Writes vendor records to QuickBooks Desktop."""

    name = "vendor"
    qbxml_entity = "Vendor"


class ItemInventorySink(QbwcItemUpsertBatchSink):
    """Writes inventory item records to QuickBooks Desktop."""

    name = "item_inventory"
    qbxml_entity = "ItemInventory"


class ItemNonInventorySink(QbwcItemUpsertBatchSink):
    """Writes non-inventory item records to QuickBooks Desktop."""

    name = "item_noninventory"
    qbxml_entity = "ItemNonInventory"


class ItemSalesTaxSink(QbwcItemUpsertBatchSink):
    """Writes sales tax item records to QuickBooks Desktop."""

    name = "item_sales_tax"
    qbxml_entity = "ItemSalesTax"


class InvoicesSink(QbwcUomTxnMixin, QbwcTxnUpsertBatchSink):
    """Writes invoice records to QuickBooks Desktop."""

    name = "invoice"
    qbxml_entity = "Invoice"


class SalesOrdersSink(QbwcTxnUpsertBatchSink):
    """Writes sales order records to QuickBooks Desktop."""

    name = "sales_order"
    qbxml_entity = "SalesOrder"


class SalesReceiptsSink(QbwcTxnUpsertBatchSink):
    """Writes sales receipt records to QuickBooks Desktop."""

    name = "sales_receipt"
    qbxml_entity = "SalesReceipt"


class CreditMemosSink(QbwcUomTxnMixin, QbwcTxnUpsertBatchSink):
    """Writes credit memo records to QuickBooks Desktop."""

    name = "credit_memo"
    qbxml_entity = "CreditMemo"


class PurchaseOrdersSink(QbwcUomTxnMixin, QbwcTxnUpsertBatchSink):
    """Writes purchase order records to QuickBooks Desktop."""

    name = "purchase_order"
    qbxml_entity = "PurchaseOrder"

    def _filter_query_matches(
        self,
        matches: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Post-filter purchase order query matches by VendorRef when RefNumber lookup is used."""
        return filter_matches_by_vendor_ref(matches, payload, self.lookup_fields)


class BillsSink(QbwcBillUpsertBatchSink):
    """Writes bill records to QuickBooks Desktop."""

    name = "bill"
    qbxml_entity = "Bill"


class VendorCreditsSink(QbwcTxnUpsertBatchSink):
    """Writes vendor credit records to QuickBooks Desktop."""

    name = "vendor_credit"
    qbxml_entity = "VendorCredit"


class JournalEntriesSink(QbwcTxnUpsertBatchSink):
    """Writes journal entry records to QuickBooks Desktop."""

    name = "journal_entry"
    qbxml_entity = "JournalEntry"
