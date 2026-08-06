"""Offline tests for QbwcBatchSink and upsert staging in client.py."""

from __future__ import annotations

from target_qbwc.sinks import CustomersSink
from target_qbwc.tests.conftest import AddOnlyCustomerSink


def test_strip_hotglue_metadata_removes_external_id_and_sdc_fields(
    customers_sink: CustomersSink,
):
    """Strip Hotglue metadata before QBXML encoding."""
    payload, external_id = customers_sink.strip_hotglue_metadata(
        {
            "externalId": "cust-001",
            "Name": "HG-TEST-001",
            "_sdc_batched_at": "2026-08-06",
            "_sdc_deleted_at": None,
        }
    )

    assert external_id == "cust-001"
    assert payload == {"Name": "HG-TEST-001"}


def test_add_only_process_batch_record_includes_request_element(
    add_only_customer_sink: AddOnlyCustomerSink,
):
    """Stage add-only records with a prebuilt CustomerAddRq request element."""
    staged = add_only_customer_sink.process_batch_record(
        {
            "externalId": "cust-001",
            "Name": "HG-TEST-001",
            "CompanyName": "Test Co",
        },
        0,
    )

    assert staged["request_id"] == "0"
    assert staged["external_id"] == "cust-001"
    assert staged["payload"] == {"Name": "HG-TEST-001", "CompanyName": "Test Co"}
    assert staged["request_element"] == {
        "CustomerAddRq": {
            "@requestID": "0",
            "CustomerAdd": {"Name": "HG-TEST-001", "CompanyName": "Test Co"},
        }
    }


def test_upsert_process_batch_record_omits_request_element(customers_sink: CustomersSink):
    """Stage upsert records without a write request element until lookup completes."""
    staged = customers_sink.process_batch_record(
        {
            "externalId": "cust-001",
            "Name": "HG-TEST-001",
            "CompanyName": "Test Co",
            "_sdc_batched_at": "2026-08-06",
        },
        0,
    )

    assert staged["request_id"] == "0"
    assert staged["external_id"] == "cust-001"
    assert staged["payload"] == {"Name": "HG-TEST-001", "CompanyName": "Test Co"}
    assert "request_element" not in staged


def test_process_batch_record_stores_overlong_name_as_preprocess_error(
    customers_sink: CustomersSink,
):
    """Stage invalid XSD records without raising so the SDK batch path stays quiet."""
    staged = customers_sink.process_batch_record(
        {
            "Name": "THIS-NAME-IS-WAY-TOO-LONG-FOR-QUICKBOOKS-CUSTOMER-FIELD",
            "CompanyName": "Invalid XSD length",
        },
        0,
    )

    assert "preprocess_error" in staged
    message = str(staged["preprocess_error"]).lower()
    assert "name" in message
    assert "41" in message
