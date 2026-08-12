"""Offline tests for QbwcBillUpsertBatchSink."""

from __future__ import annotations

from unittest.mock import patch

from target_qbwc.bill_lines import preprocess_bill_add_payload
from target_qbwc.sinks import BillsSink
from target_qbwc.tests.conftest import make_sink


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


def test_bill_vendor_ref_filter(target_config):
    """Post-filter bill query matches by VendorRef."""
    sink = make_sink(BillsSink, target_config)
    matches = [
        {
            "TxnID": "1",
            "EditSequence": "1",
            "VendorRef": {"ListID": "V1", "FullName": "Vendor A"},
        },
        {
            "TxnID": "2",
            "EditSequence": "2",
            "VendorRef": {"ListID": "V2", "FullName": "Vendor B"},
        },
    ]

    filtered = sink._filter_query_matches(
        matches,
        {"RefNumber": "BILL-001", "VendorRef": {"ListID": "V2"}},
    )

    assert len(filtered) == 1
    assert filtered[0]["TxnID"] == "2"


def test_merge_for_mod_header_only_preserves_generic_behavior(bills_sink: BillsSink):
    """Skip line reconciliation when the payload has no line arrays."""
    merged = bills_sink._merge_for_mod(
        _bill_ret(),
        {"Memo": "Header only"},
    )

    assert merged["Memo"] == "Header only"
    assert "ExpenseLineMod" not in merged
    assert "ClearExpenseLines" not in merged
    assert "VendorAddress" not in merged


def test_build_lookup_query_element_sets_include_line_items(bills_sink: BillsSink):
    """Request line items on bill lookup queries."""
    query = bills_sink._build_lookup_query_element(
        {"RefNumber": "BILL-REF-001"},
        "0",
    )

    assert query == {
        "BillQueryRq": {
            "@requestID": "0",
            "RefNumber": "BILL-REF-001",
            "IncludeLineItems": "true",
        },
    }


def test_build_write_request_mod_attaches_line_external_ids(bills_sink: BillsSink):
    """Attach mod line externalId lists to the staged write request."""
    staged = {
        "request_id": "0",
        "payload": {
            "RefNumber": "BILL-REF-001",
            "ExpenseLineAdd": [
                {"TxnLineID": "1", "Amount": "35.00", "externalId": "exp-upd-1"},
            ],
        },
    }

    write_request = bills_sink._build_write_request(
        staged,
        {"matches": [_bill_ret()], "query_failed": False},
    )

    assert write_request["write_op"] == "mod"
    assert write_request["bill_expense_line_external_ids"] == ["exp-upd-1"]


def test_handle_batch_response_add_includes_custom_data(bills_sink: BillsSink):
    """Return line customData on successful bill add responses."""
    payload = {
        "VendorRef": {"FullName": "Vendor A"},
        "ExpenseLineAdd": [{"externalId": "exp-1", "Amount": "30.00"}],
    }
    prepared, item_ids, expense_ids = preprocess_bill_add_payload(payload)
    staged = {
        "request_id": "0",
        "external_id": "bill-add",
        "payload": prepared,
        "write_op": "add",
        "bill_item_line_external_ids": item_ids,
        "bill_expense_line_external_ids": expense_ids,
    }

    result = bills_sink.handle_batch_response(
        {
            "items": [
                {
                    "record": {**staged, "write_op": "add"},
                    "response": {
                        "request_id": "0",
                        "status_code": "0",
                        "entity": {
                            "TxnID": "BILL-NEW",
                            "ExpenseLineRet": {"TxnLineID": "99"},
                        },
                    },
                }
            ]
        }
    )

    update = result["state_updates"][0]
    assert update["success"] is True
    assert update["customData"] == {
        "expenseLines": [{"externalId": "exp-1", "id": "99"}],
    }


def test_handle_batch_response_mod_includes_custom_data(bills_sink: BillsSink):
    """Return line customData on successful bill mod responses."""
    staged = {
        "request_id": "0",
        "external_id": "bill-mod",
        "payload": {"Memo": "Updated"},
        "write_op": "mod",
        "bill_expense_line_external_ids": ["exp-upd-1", "exp-new-1"],
        "bill_item_line_external_ids": [],
    }

    result = bills_sink.handle_batch_response(
        {
            "items": [
                {
                    "record": staged,
                    "response": {
                        "request_id": "0",
                        "status_code": "0",
                        "entity": {
                            "TxnID": "BILL-001",
                            "ExpenseLineRet": [
                                {"TxnLineID": "10"},
                                {"TxnLineID": "11"},
                            ],
                        },
                    },
                }
            ]
        }
    )

    update = result["state_updates"][0]
    assert update["is_updated"] is True
    assert update["customData"]["expenseLines"] == [
        {"externalId": "exp-upd-1", "id": "10"},
        {"externalId": "exp-new-1", "id": "11"},
    ]


def test_make_batch_request_runs_include_line_items_lookup_then_mod(bills_sink: BillsSink):
    """Run bill lookup with IncludeLineItems before a mod write."""
    staged = bills_sink.process_batch_record(
        {
            "externalId": "bill-mod",
            "TxnID": "BILL-001",
            "ExpenseLineAdd": [
                {"TxnLineID": "1", "Amount": "35.00", "externalId": "exp-upd-1"},
            ],
        },
        0,
    )
    query_response = {
        "BillQueryRs": {
            "@requestID": "0",
            "@statusCode": "0",
            "BillRet": _bill_ret(),
        }
    }
    write_response = {
        "BillModRs": {
            "@requestID": "0",
            "@statusCode": "0",
            "BillRet": {
                "TxnID": "BILL-001",
                "ExpenseLineRet": [{"TxnLineID": "10"}],
            },
        }
    }

    with patch.object(
        bills_sink,
        "send_qbxml_batch",
        side_effect=[query_response, write_response],
    ) as send_batch:
        result = bills_sink.make_batch_request([staged])

    lookup_request = send_batch.call_args_list[0].args[0][0]
    assert lookup_request["BillQueryRq"]["IncludeLineItems"] == "true"
    item = result["items"][0]
    assert item["record"]["write_op"] == "mod"
    mod_payload = item["record"]["request_element"]["BillModRq"]["BillMod"]
    assert mod_payload["ExpenseLineMod"] == [
        {"TxnLineID": "1", "Amount": "35.00", "Memo": "Line 1"},
    ]


def test_build_write_request_add_preprocesses_lines(bills_sink: BillsSink):
    """Strip line TxnLineIDs during add write staging."""
    staged = {
        "request_id": "0",
        "payload": {
            "VendorRef": {"FullName": "Vendor A"},
            "ExpenseLineAdd": [{"TxnLineID": "stale", "Amount": "30.00"}],
        },
    }

    write_request = bills_sink._build_write_request(
        staged,
        {"matches": [], "query_failed": False},
    )

    add_payload = write_request["request_element"]["BillAddRq"]["BillAdd"]
    assert "TxnLineID" not in add_payload["ExpenseLineAdd"][0]
    assert write_request["write_op"] == "add"
