"""QBWC target sink classes."""

from __future__ import annotations

from typing import Any

from target_qbwc.bill_sink import QbwcBillUpsertBatchSink
from target_qbwc.uom_sink import QbwcUomTxnMixin
from target_qbwc.client_upsert import QbwcListUpsertBatchSink, QbwcTxnUpsertBatchSink, filter_matches_by_vendor_ref


class CustomersSink(QbwcListUpsertBatchSink):
    """Writes customer records to QuickBooks Desktop."""

    name = "customer"
    qbxml_entity = "Customer"


class VendorsSink(QbwcListUpsertBatchSink):
    """Writes vendor records to QuickBooks Desktop."""

    name = "vendor"
    qbxml_entity = "Vendor"


class ItemInventorySink(QbwcListUpsertBatchSink):
    """Writes inventory item records to QuickBooks Desktop."""

    name = "item_inventory"
    qbxml_entity = "ItemInventory"


class ItemNonInventorySink(QbwcListUpsertBatchSink):
    """Writes non-inventory item records to QuickBooks Desktop."""

    name = "item_noninventory"
    qbxml_entity = "ItemNonInventory"


class ItemSalesTaxSink(QbwcListUpsertBatchSink):
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
