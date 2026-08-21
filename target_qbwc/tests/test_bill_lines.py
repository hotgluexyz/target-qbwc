"""Offline tests for bill line preprocessing, mod reconciliation and customData."""

from __future__ import annotations

from target_qbwc.bill_lines import (
    BillLineMapping,
    BillLineRef,
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


def _external_ids(mapping: BillLineMapping) -> list[str | None]:
    """Return collected externalIds from a line mapping."""
    return [ref.external_id for ref in mapping.refs]


def test_preprocess_bill_add_strips_txn_line_id_and_collects_external_ids():
    """Strip line TxnLineIDs on add and keep index-aligned externalIds including gaps."""
    payload = {
        "VendorRef": {"FullName": "Vendor A"},
        "ExpenseLineAdd": [
            {"TxnLineID": "stale", "externalId": "exp-1", "Amount": "30.00"},
            {"Amount": "10.00"},
            {"externalId": "exp-3", "Amount": "5.00"},
        ],
    }

    prepared, item_mapping, expense_mapping = preprocess_bill_add_payload(payload)

    assert _external_ids(item_mapping) == []
    assert _external_ids(expense_mapping) == ["exp-1", None, "exp-3"]
    assert expense_mapping.existing_txn_line_ids == frozenset()
    assert "TxnLineID" not in prepared["ExpenseLineAdd"][0]
    assert "externalId" not in prepared["ExpenseLineAdd"][0]
    assert "externalId" not in prepared["ExpenseLineAdd"][2]


def test_build_bill_line_custom_data_maps_add_lines_by_position():
    """Map add-line externalIds to response lines by position, skipping untracked lines."""
    custom_data = build_bill_line_custom_data(
        {
            "TxnID": "BILL-001",
            "ExpenseLineRet": [
                {"TxnLineID": "10"},
                {"TxnLineID": "11"},
                {"TxnLineID": "12"},
            ],
        },
        BillLineMapping(refs=(), existing_txn_line_ids=frozenset()),
        BillLineMapping(
            refs=(
                BillLineRef("exp-1", None),
                BillLineRef(None, None),
                BillLineRef("exp-3", None),
            ),
            existing_txn_line_ids=frozenset(),
        ),
    )

    assert custom_data["expenseLines"] == [
        {"externalId": "exp-1", "id": "10"},
        {"externalId": "exp-3", "id": "12"},
    ]


def test_build_bill_line_custom_data_maps_mod_lines_by_txn_line_id():
    """Match existing mod lines by TxnLineID and new lines to leftover response ids."""
    custom_data = build_bill_line_custom_data(
        {
            "TxnID": "BILL-001",
            "ExpenseLineRet": [
                {"TxnLineID": "1"},
                {"TxnLineID": "2"},
                {"TxnLineID": "99"},
            ],
        },
        BillLineMapping(refs=(), existing_txn_line_ids=frozenset()),
        BillLineMapping(
            refs=(
                BillLineRef("exp-upd-2", "2"),
                BillLineRef("exp-new-1", "-1"),
            ),
            existing_txn_line_ids=frozenset({"1", "2"}),
        ),
    )

    assert custom_data["expenseLines"] == [
        {"externalId": "exp-upd-2", "id": "2"},
        {"externalId": "exp-new-1", "id": "99"},
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

    merged, item_mapping, expense_mapping = merge_bill_for_mod(
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
    assert _external_ids(expense_mapping) == ["exp-upd-1", "exp-new-1"]
    assert [ref.txn_line_id for ref in expense_mapping.refs] == ["1", "-1"]
    assert expense_mapping.existing_txn_line_ids == frozenset({"1", "2"})
    assert _external_ids(item_mapping) == []


def test_merge_bill_for_mod_clears_lines_when_payload_side_is_empty(bills_sink: BillsSink):
    """Set ClearExpenseLines when incoming ExpenseLineAdd is an empty list."""
    merged, _, expense_mapping = merge_bill_for_mod(
        _bill_ret(),
        {"ExpenseLineAdd": []},
        bills_sink.qbd_xml_schemas,
        "TxnID",
    )

    assert merged["ClearExpenseLines"] == "true"
    assert "ExpenseLineMod" not in merged
    assert _external_ids(expense_mapping) == []


def test_merge_bill_for_mod_drops_unmatched_existing_lines(bills_sink: BillsSink):
    """Omit existing lines that are not referenced in the incoming payload."""
    merged, _, expense_mapping = merge_bill_for_mod(
        _bill_ret(),
        {
            "ExpenseLineAdd": [
                {"TxnLineID": "2", "Amount": "20.00", "externalId": "exp-2"},
            ],
        },
        bills_sink.qbd_xml_schemas,
        "TxnID",
    )

    assert merged["ExpenseLineMod"] == [
        {"TxnLineID": "2", "Amount": "20.00", "Memo": "Line 2"},
    ]
    assert _external_ids(expense_mapping) == ["exp-2"]
    assert expense_mapping.refs[0].txn_line_id == "2"
