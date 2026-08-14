"""Offline tests for QbwcUomTxnMixin batch integration."""

from __future__ import annotations

from unittest.mock import patch

from hotglue_singer_sdk.exceptions import FatalAPIError

from target_qbwc.sinks import InvoicesSink
from target_qbwc.target import TargetQbwc
from target_qbwc.tests.conftest import uom_item, uom_set


def _write_batch_results(staged_records: list[dict]) -> list[dict]:
    """Return one success item per staged write record."""
    return [{"record": record, "response": None} for record in staged_records]


def test_make_batch_request_applies_uom_rescaling_before_write(invoices_sink):
    """Apply UOM rescaling during make_batch_request before lookup queries."""
    staged = {
        "request_id": "0",
        "external_id": "inv-uom-1",
        "payload": {
            "RefNumber": "INV-UOM-1",
            "CustomerRef": {"FullName": "Customer A"},
            "InvoiceLineAdd": [
                {
                    "Quantity": "4",
                    "ItemRef": {"FullName": "4080K"},
                    "UnitOfMeasure": "case",
                }
            ],
        },
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    with patch.object(invoices_sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}]) as lookup_mock, patch.object(
        invoices_sink,
        "_execute_write_batch",
        return_value=[{"record": staged, "response": None}],
    ) as write_mock, patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=[
            {"ItemQueryRs": [{"@requestID": "uom-item-1", "@statusCode": "0", "ItemInventoryRet": item}]},
            {"UnitOfMeasureSetQueryRs": [{"@requestID": "uom-uom-set-1", "@statusCode": "0", "UnitOfMeasureSetRet": uom_set_fixture}]},
        ],
    ) as send_mock:
        invoices_sink.make_batch_request([staged])

    assert send_mock.call_count == 2
    assert len(send_mock.call_args_list[0].args[0]) == 1
    lookup_mock.assert_called_once()
    write_mock.assert_called_once()
    line = staged["payload"]["InvoiceLineAdd"][0]
    assert line["Quantity"] == "40"


def test_make_batch_request_batches_unique_items_in_one_uom_lookup(invoices_sink):
    """Query each unique item once inside a single UOM item lookup batch."""
    item = uom_item()
    uom_set_fixture = uom_set()
    staged_records = [
        {
            "request_id": str(index),
            "external_id": f"inv-{index}",
            "payload": {
                "RefNumber": f"INV-{index}",
                "CustomerRef": {"FullName": "Customer A"},
                "InvoiceLineAdd": [
                    {
                        "Quantity": "2",
                        "ItemRef": {"FullName": "4080K"},
                        "UnitOfMeasure": "case",
                    }
                ],
            },
        }
        for index in range(3)
    ]

    with patch.object(invoices_sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}] * 3), patch.object(
        invoices_sink,
        "_execute_write_batch",
        return_value=[{"record": staged, "response": None} for staged in staged_records],
    ), patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=[
            {
                "ItemQueryRs": [
                    {
                        "@requestID": f"uom-item-{index}",
                        "@statusCode": "0",
                        "ItemInventoryRet": item,
                    }
                    for index in range(1, 2)
                ]
            },
            {
                "UnitOfMeasureSetQueryRs": [
                    {
                        "@requestID": "uom-uom-set-1",
                        "@statusCode": "0",
                        "UnitOfMeasureSetRet": uom_set_fixture,
                    }
                ]
            },
        ],
    ) as send_mock:
        invoices_sink.make_batch_request(staged_records)

    item_batch = send_mock.call_args_list[0].args[0]
    assert len(item_batch) == 1


def test_make_batch_request_skips_item_lookup_for_lines_without_quantity(invoices_sink):
    """Do not query items for lines that omit Quantity."""
    staged = {
        "request_id": "0",
        "external_id": "inv-no-qty",
        "payload": {
            "RefNumber": "INV-NO-QTY",
            "CustomerRef": {"FullName": "Customer A"},
            "InvoiceLineAdd": [
                {"ItemRef": {"FullName": "4080K"}, "Amount": "10.00"},
            ],
        },
    }

    with patch.object(invoices_sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}]), patch.object(
        invoices_sink,
        "_execute_write_batch",
        return_value=[{"record": staged, "response": None}],
    ), patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
    ) as send_mock:
        invoices_sink.make_batch_request([staged])

    send_mock.assert_not_called()


def test_target_uom_cache_reuses_items_across_batches():
    """Reuse target-level UOM cache across successive SDK batches."""
    target = TargetQbwc(config={"token": "test-token", "is_sandbox": True})
    sink = InvoicesSink(
        target=target,
        stream_name="invoice",
        schema={"type": "object", "properties": {}},
        key_properties=["externalId"],
    )
    item = uom_item()
    uom_set_fixture = uom_set()

    def _staged(record_id: str) -> dict:
        return {
            "request_id": record_id,
            "external_id": record_id,
            "payload": {
                "RefNumber": f"INV-{record_id}",
                "CustomerRef": {"FullName": "Customer A"},
                "InvoiceLineAdd": [
                    {
                        "Quantity": "1",
                        "ItemRef": {"FullName": "4080K"},
                        "UnitOfMeasure": "case",
                    }
                ],
            },
        }

    with patch.object(sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}]), patch.object(
        sink,
        "_execute_write_batch",
        side_effect=lambda staged: [{"record": record, "response": None} for record in staged],
    ), patch.object(
        sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        sink,
        "send_qbxml_batch",
        side_effect=[
            {"ItemQueryRs": [{"@requestID": "uom-item-1", "@statusCode": "0", "ItemInventoryRet": item}]},
            {"UnitOfMeasureSetQueryRs": [{"@requestID": "uom-uom-set-1", "@statusCode": "0", "UnitOfMeasureSetRet": uom_set_fixture}]},
            AssertionError("second batch should not query UOM data"),
        ],
    ) as send_mock:
        sink.make_batch_request([_staged("0")])
        sink.make_batch_request([_staged("1")])

    assert send_mock.call_count == 2


def test_make_batch_request_records_invalid_uom_as_preprocess_error(invoices_sink):
    """Surface invalid UOM names as per-record preprocess_error state."""
    staged = {
        "request_id": "0",
        "external_id": "inv-uom-bad",
        "payload": {
            "RefNumber": "INV-UOM-BAD",
            "CustomerRef": {"FullName": "Customer A"},
            "InvoiceLineAdd": [
                {
                    "Quantity": "4",
                    "ItemRef": {"FullName": "4080K"},
                    "UnitOfMeasure": "pallet",
                }
            ],
        },
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    with patch.object(invoices_sink, "_execute_lookup_queries") as lookup_mock, patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=[
            {"ItemQueryRs": [{"@requestID": "uom-item-1", "@statusCode": "0", "ItemInventoryRet": item}]},
            {"UnitOfMeasureSetQueryRs": [{"@requestID": "uom-uom-set-1", "@statusCode": "0", "UnitOfMeasureSetRet": uom_set_fixture}]},
        ],
    ):
        result = invoices_sink.make_batch_request([staged])

    lookup_mock.assert_not_called()
    item_result = result["items"][0]
    assert "preprocess_error" in item_result
    assert "UnitOfMeasure 'pallet' is not valid" in str(item_result["preprocess_error"])


def test_make_batch_request_bill_uom_preserves_line_external_ids(bills_sink):
    """Collect bill line externalIds after UOM rescaling for customData mapping."""
    staged = {
        "request_id": "0",
        "external_id": "bill-uom-line",
        "payload": {
            "RefNumber": "BILL-UOM-LINE",
            "VendorRef": {"FullName": "Vendor A"},
            "ItemLineAdd": [
                {
                    "Quantity": "2",
                    "ItemRef": {"FullName": "4080K"},
                    "UnitOfMeasure": "case",
                    "externalId": "item-line-1",
                }
            ],
        },
    }
    item = uom_item()
    uom_set_fixture = uom_set()
    write_staged: list[dict] = []

    def capture_write_batch(staged_records: list[dict]) -> list[dict]:
        write_staged.extend(staged_records)
        return [{"record": record, "response": None} for record in staged_records]

    with patch.object(bills_sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}]), patch.object(
        bills_sink,
        "_execute_write_batch",
        side_effect=capture_write_batch,
    ), patch.object(
        bills_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        bills_sink,
        "send_qbxml_batch",
        side_effect=[
            {"ItemQueryRs": [{"@requestID": "uom-item-1", "@statusCode": "0", "ItemInventoryRet": item}]},
            {"UnitOfMeasureSetQueryRs": [{"@requestID": "uom-uom-set-1", "@statusCode": "0", "UnitOfMeasureSetRet": uom_set_fixture}]},
        ],
    ):
        bills_sink.make_batch_request([staged])

    assert len(write_staged) == 1
    assert write_staged[0]["bill_item_line_mapping"].refs[0].external_id == "item-line-1"
    assert write_staged[0]["payload"]["ItemLineAdd"][0]["Quantity"] == "20"


def test_make_batch_request_uom_item_lookup_failure_fails_record(invoices_sink):
    """Fail records when UOM item lookup transport fails, without caching a miss."""

    def _staged(request_id: str, external_id: str) -> dict:
        return {
            "request_id": request_id,
            "external_id": external_id,
            "payload": {
                "RefNumber": f"INV-{external_id}",
                "CustomerRef": {"FullName": "Customer A"},
                "InvoiceLineAdd": [
                    {
                        "Quantity": "2",
                        "ItemRef": {"FullName": "4080K"},
                        "UnitOfMeasure": "case",
                    }
                ],
            },
        }

    first_staged = _staged("0", "inv-uom-miss")
    second_staged = _staged("1", "inv-uom-miss-2")

    with patch.object(invoices_sink, "_execute_lookup_queries") as lookup_mock, patch.object(
        invoices_sink,
        "_execute_write_batch",
        side_effect=_write_batch_results,
    ) as write_mock, patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=RuntimeError("transport down"),
    ) as send_mock:
        first = invoices_sink.make_batch_request([first_staged])
        second = invoices_sink.make_batch_request([second_staged])

    assert send_mock.call_count == 2
    lookup_mock.assert_not_called()
    write_mock.assert_not_called()
    first_error = first["items"][0]["preprocess_error"]
    assert isinstance(first_error, FatalAPIError)
    assert "transport down" in str(first_error)
    assert isinstance(second["items"][0]["preprocess_error"], FatalAPIError)
    assert first_staged["payload"]["InvoiceLineAdd"][0]["Quantity"] == "2"

    first_update = invoices_sink.handle_batch_response(first)["state_updates"][0]
    assert first_update["success"] is False
    assert "hg_error_class" not in first_update


def test_make_batch_request_uom_set_lookup_failure_fails_record(invoices_sink):
    """Fail records when UOM set lookup transport fails, without caching a miss."""
    staged = {
        "request_id": "0",
        "external_id": "inv-uom-set-miss",
        "payload": {
            "RefNumber": "INV-UOM-SET-MISS",
            "CustomerRef": {"FullName": "Customer A"},
            "InvoiceLineAdd": [
                {
                    "Quantity": "2",
                    "ItemRef": {"FullName": "4080K"},
                    "UnitOfMeasure": "case",
                }
            ],
        },
    }
    item = uom_item()

    with patch.object(invoices_sink, "_execute_lookup_queries") as lookup_mock, patch.object(
        invoices_sink,
        "_execute_write_batch",
        side_effect=_write_batch_results,
    ) as write_mock, patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=[
            {"ItemQueryRs": [{"@requestID": "uom-item-1", "@statusCode": "0", "ItemInventoryRet": item}]},
            RuntimeError("transport down"),
        ],
    ):
        result = invoices_sink.make_batch_request([staged])

    lookup_mock.assert_not_called()
    write_mock.assert_not_called()
    error = result["items"][0]["preprocess_error"]
    assert isinstance(error, FatalAPIError)
    assert "transport down" in str(error)
    assert staged["payload"]["InvoiceLineAdd"][0]["Quantity"] == "2"

    update = invoices_sink.handle_batch_response(result)["state_updates"][0]
    assert update["success"] is False
    assert "hg_error_class" not in update


def test_make_batch_request_uom_item_lookup_zero_matches_marks_cache_miss(invoices_sink):
    """Treat empty item query results as cache misses and skip rescaling."""
    staged = {
        "request_id": "0",
        "external_id": "inv-uom-empty",
        "payload": {
            "RefNumber": "INV-UOM-EMPTY",
            "CustomerRef": {"FullName": "Customer A"},
            "InvoiceLineAdd": [
                {
                    "Quantity": "2",
                    "ItemRef": {"FullName": "4080K"},
                    "UnitOfMeasure": "case",
                }
            ],
        },
    }

    with patch.object(invoices_sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}]), patch.object(
        invoices_sink,
        "_execute_write_batch",
        side_effect=_write_batch_results,
    ), patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=[
            {"ItemQueryRs": [{"@requestID": "uom-item-1", "@statusCode": "0"}]},
            AssertionError("UOM set lookup should not run when item is missing"),
        ],
    ) as send_mock:
        invoices_sink.make_batch_request([staged])
        invoices_sink.make_batch_request([dict(staged, request_id="1", external_id="inv-uom-empty-2")])

    assert send_mock.call_count == 1
    assert staged["payload"]["InvoiceLineAdd"][0]["Quantity"] == "2"


def test_make_batch_request_uom_item_lookup_multiple_matches_marks_cache_miss(invoices_sink):
    """Treat ambiguous item query results as cache misses and skip rescaling."""
    staged = {
        "request_id": "0",
        "external_id": "inv-uom-ambig",
        "payload": {
            "RefNumber": "INV-UOM-AMBIG",
            "CustomerRef": {"FullName": "Customer A"},
            "InvoiceLineAdd": [
                {
                    "Quantity": "2",
                    "ItemRef": {"FullName": "4080K"},
                    "UnitOfMeasure": "case",
                }
            ],
        },
    }
    item = uom_item()

    with patch.object(invoices_sink, "_execute_lookup_queries", return_value=[{"matches": [], "query_failed": False}]), patch.object(
        invoices_sink,
        "_execute_write_batch",
        side_effect=_write_batch_results,
    ), patch.object(
        invoices_sink,
        "_validate_request_element",
        return_value=None,
    ), patch.object(
        invoices_sink,
        "send_qbxml_batch",
        side_effect=[
            {
                "ItemQueryRs": [
                    {
                        "@requestID": "uom-item-1",
                        "@statusCode": "0",
                        "ItemInventoryRet": [item, dict(item, ListID="ITEM-002")],
                    }
                ]
            },
            AssertionError("UOM set lookup should not run when item lookup is ambiguous"),
        ],
    ) as send_mock:
        invoices_sink.make_batch_request([staged])
        invoices_sink.make_batch_request([dict(staged, request_id="1", external_id="inv-uom-ambig-2")])

    assert send_mock.call_count == 1
    assert staged["payload"]["InvoiceLineAdd"][0]["Quantity"] == "2"
