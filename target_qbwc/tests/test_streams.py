"""Offline tests for write stream sink configuration."""

from __future__ import annotations

import pytest

from target_qbwc.sinks import (
    BillsSink,
    CreditMemosSink,
    CustomersSink,
    InvoicesSink,
    ItemInventorySink,
    ItemNonInventorySink,
    ItemSalesTaxSink,
    JournalEntriesSink,
    SalesOrdersSink,
    SalesReceiptsSink,
    VendorCreditsSink,
    VendorsSink,
)
from target_qbwc.tests.conftest import make_sink

LIST_SINKS = [
    (CustomersSink, "customer", "Customer", "ListID"),
    (VendorsSink, "vendor", "Vendor", "ListID"),
    (ItemInventorySink, "item_inventory", "ItemInventory", "ListID"),
    (ItemNonInventorySink, "item_noninventory", "ItemNonInventory", "ListID"),
    (ItemSalesTaxSink, "item_sales_tax", "ItemSalesTax", "ListID"),
]

TXN_SINKS = [
    (InvoicesSink, "invoice", "Invoice", "TxnID"),
    (SalesOrdersSink, "sales_order", "SalesOrder", "TxnID"),
    (SalesReceiptsSink, "sales_receipt", "SalesReceipt", "TxnID"),
    (CreditMemosSink, "credit_memo", "CreditMemo", "TxnID"),
    (BillsSink, "bill", "Bill", "TxnID"),
    (VendorCreditsSink, "vendor_credit", "VendorCredit", "TxnID"),
    (JournalEntriesSink, "journal_entry", "JournalEntry", "TxnID"),
]


@pytest.mark.parametrize(("sink_cls", "name", "entity", "id_field"), LIST_SINKS + TXN_SINKS)
def test_sink_qbxml_element_names(sink_cls, name, entity, id_field, target_config):
    """Derive QBXML request and response element names from qbxml_entity."""
    sink = make_sink(sink_cls, target_config)

    assert sink.name == name
    assert sink.qbxml_entity == entity
    assert sink.id_field == id_field
    assert sink.request_element_name == f"{entity}AddRq"
    assert sink.response_element_name == f"{entity}AddRs"
    assert sink.entity_add_name == f"{entity}Add"
    assert sink.query_request_element_name == f"{entity}QueryRq"
    assert sink.query_response_element_name == f"{entity}QueryRs"
    assert sink.query_ret_element_name == f"{entity}Ret"
    assert sink.mod_request_element_name == f"{entity}ModRq"
    assert sink.mod_response_element_name == f"{entity}ModRs"
    assert sink.entity_mod_name == f"{entity}Mod"


@pytest.mark.parametrize(("sink_cls", "name", "entity", "id_field"), LIST_SINKS)
def test_list_sink_lookup_fields(sink_cls, name, entity, id_field, target_config):
    """List sinks look up by ListID then Name."""
    sink = make_sink(sink_cls, target_config)
    assert sink.lookup_fields == [("ListID", "ListID"), ("Name", "FullName")]


@pytest.mark.parametrize(("sink_cls", "name", "entity", "id_field"), TXN_SINKS)
def test_txn_sink_lookup_fields(sink_cls, name, entity, id_field, target_config):
    """Transaction sinks look up by TxnID then RefNumber."""
    sink = make_sink(sink_cls, target_config)
    assert sink.lookup_fields == [("TxnID", "TxnID"), ("RefNumber", "RefNumber")]


def test_merge_for_mod_uses_txn_id(target_config):
    """Transactional merge keeps TxnID and EditSequence from the queried record."""
    sink = make_sink(InvoicesSink, target_config)
    merged = sink._merge_for_mod(
        {"TxnID": "ABC-123", "EditSequence": "99", "RefNumber": "INV-1"},
        {"Memo": "Updated memo"},
    )

    assert merged["TxnID"] == "ABC-123"
    assert merged["EditSequence"] == "99"
    assert merged["Memo"] == "Updated memo"
    assert merged["RefNumber"] == "INV-1"
