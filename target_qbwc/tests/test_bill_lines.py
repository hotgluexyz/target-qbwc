"""Offline tests for bill line preprocessing, mod reconciliation and customData."""

from __future__ import annotations

from target_qbwc.bill_lines import (
    build_bill_line_custom_data,
    merge_bill_for_mod,
    preprocess_bill_add_payload,
)
from target_qbwc.sinks import BillsSink


def _bill_ret(**fields) -> dict:
    """Build a minimal BillRet fixture with two expense lines."""
    base = {
        "TxnID": "BILL-001",
        "EditSequence": "42",
        "RefNumber": "BILL-REF-001",
        "VendorRef": {"ListID": "V1", "FullName": "Vendor A"},
        "VendorAddress": {"Addr1": "123 Vendor St"},
        "Memo": "Original memo",
        "ExpenseLineRet": [
            {"TxnLineID": "1", "Amount": "30.00", "Memo": "Line 1"},
            {"TxnLineID": "2", "Amount": "15.00", "Memo": "Line 2"},
        ],
    }
    base.update(fields)
    return base


def test_preprocess_bill_add_strips_txn_line_id_and_collects_external_ids():
    """Strip line TxnLineIDs on add and collect truthy line externalIds."""
    payload = {
        "VendorRef": {"FullName": "Vendor A"},
        "ExpenseLineAdd": [
            {"TxnLineID": "stale", "externalId": "exp-1", "Amount": "30.00"},
            {"Amount": "10.00"},
            {"externalId": "exp-3", "Amount": "5.00"},
        ],
    }

    prepared, item_ids, expense_ids = preprocess_bill_add_payload(payload)

    assert item_ids == []
    assert expense_ids == ["exp-1", "exp-3"]
    assert "TxnLineID" not in prepared["ExpenseLineAdd"][0]
    assert "externalId" not in prepared["ExpenseLineAdd"][0]
    assert "externalId" not in prepared["ExpenseLineAdd"][2]


def test_build_bill_line_custom_data_maps_response_lines():
    """Map index-aligned externalIds to QB response line TxnLineIDs."""
    custom_data = build_bill_line_custom_data(
        {
            "TxnID": "BILL-001",
            "ExpenseLineRet": [
                {"TxnLineID": "10"},
                {"TxnLineID": "11"},
            ],
        },
        [],
        ["exp-1", None, "exp-3"],
    )

    assert custom_data["expenseLines"] == [
        {"externalId": "exp-1", "id": "10"},
    ]


def test_merge_bill_for_mod_updates_existing_and_adds_new_lines(bills_sink: BillsSink):
    """Reconcile mod lines by TxnLineID, add new lines with TxnLineID -1."""
    incoming = {
        "Memo": "Updated memo",
        "ExpenseLineAdd": [
            {"TxnLineID": "1", "Amount": "35.00", "externalId": "exp-upd-1"},
            {"Amount": "7.00", "externalId": "exp-new-1"},
        ],
    }

    merged, item_ids, expense_ids = merge_bill_for_mod(
        _bill_ret(),
        incoming,
        bills_sink.qbd_xml_schemas,
        "TxnID",
    )

    assert merged["TxnID"] == "BILL-001"
    assert merged["EditSequence"] == "42"
    assert merged["Memo"] == "Updated memo"
    assert "VendorAddress" not in merged
    assert merged["ExpenseLineMod"] == [
        {"TxnLineID": "1", "Amount": "35.00", "Memo": "Line 1"},
        {"TxnLineID": "-1", "Amount": "7.00"},
    ]
    assert "ClearExpenseLines" not in merged
    assert expense_ids == ["exp-upd-1", "exp-new-1"]
    assert item_ids == []


def test_merge_bill_for_mod_clears_lines_when_payload_side_is_empty(bills_sink: BillsSink):
    """Set ClearExpenseLines when incoming ExpenseLineAdd is an empty list."""
    merged, _, expense_ids = merge_bill_for_mod(
        _bill_ret(),
        {"ExpenseLineAdd": []},
        bills_sink.qbd_xml_schemas,
        "TxnID",
    )

    assert merged["ClearExpenseLines"] == "true"
    assert "ExpenseLineMod" not in merged
    assert expense_ids == []


def test_merge_bill_for_mod_drops_unmatched_existing_lines(bills_sink: BillsSink):
    """Omit existing lines that are not referenced in the incoming payload."""
    merged, _, _ = merge_bill_for_mod(
        _bill_ret(),
        {
            "ExpenseLineAdd": [
                {"TxnLineID": "2", "Amount": "20.00"},
            ],
        },
        bills_sink.qbd_xml_schemas,
        "TxnID",
    )

    assert merged["ExpenseLineMod"] == [
        {"TxnLineID": "2", "Amount": "20.00", "Memo": "Line 2"},
    ]
