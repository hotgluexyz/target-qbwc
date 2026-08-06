"""Offline tests for CustomersSink batch response handling."""

from __future__ import annotations

from pathlib import Path

from hotglue_etl_exceptions import InvalidPayloadError
from qbwc_common import (
    decode_response,
    load_qbd_xml_schemas,
    normalize_rs_list,
    parse_rs_element,
)

from target_qbwc.sinks import CustomersSink

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SCHEMAS = load_qbd_xml_schemas()


def _load_parsed_responses(fixture_name: str, rs_element: str = "CustomerAddRs"):
    """Decode a response fixture and return parsed *Rs elements."""
    response_xml = (FIXTURES_DIR / fixture_name).read_text()
    body = decode_response(response_xml, SCHEMAS, validation="skip")
    return [parse_rs_element(record) for record in normalize_rs_list(body, rs_element)]


def _staged_record(
    sink: CustomersSink,
    *,
    request_id: str,
    external_id: str,
    name: str,
    write_op: str = "add",
) -> dict:
    """Build a staged batch record matching upsert write staging."""
    payload = {"Name": name, "CompanyName": name}
    response_element = (
        sink.mod_response_element_name if write_op == "mod" else sink.response_element_name
    )
    request_element_name = (
        sink.mod_request_element_name if write_op == "mod" else sink.request_element_name
    )
    entity_name = sink.entity_mod_name if write_op == "mod" else sink.entity_add_name
    return {
        "request_id": request_id,
        "external_id": external_id,
        "payload": payload,
        "write_op": write_op,
        "write_response_element": response_element,
        "request_element": {
            request_element_name: {
                "@requestID": request_id,
                entity_name: payload,
            }
        },
    }


def test_handle_batch_response_mixed_success_and_failure(customers_sink: CustomersSink):
    """Map a mixed QuickBooks batch onto per-record success and failure states."""
    parsed = _load_parsed_responses("customer-batch-mixed.response.xml")
    staged = [
        _staged_record(customers_sink, request_id="ext-mix-1", external_id="ext-1", name="HG-BATCH-MIX-1"),
        _staged_record(customers_sink, request_id="ext-mix-2", external_id="ext-2", name="HG-BATCH-MIX-1"),
        _staged_record(customers_sink, request_id="ext-mix-3", external_id="ext-3", name="HG-BATCH-MIX-3"),
        _staged_record(customers_sink, request_id="ext-mix-4", external_id="ext-4", name="HG-BATCH-MIX-4"),
        _staged_record(customers_sink, request_id="ext-mix-5", external_id="ext-5", name="HG-BATCH-MIX-5"),
    ]
    items = [{"record": record, "response": parsed[i]} for i, record in enumerate(staged)]

    result = customers_sink.handle_batch_response({"items": items})
    updates = result["state_updates"]

    assert len(updates) == 5
    assert updates[0]["success"] is True
    assert updates[0]["externalId"] == "ext-1"
    assert updates[0]["id"] == "8000274C-1785964190"

    assert updates[1]["success"] is False
    assert updates[1]["hg_error_class"] == InvalidPayloadError.__name__
    assert "[3100]" in updates[1]["error"]
    assert "already in use" in updates[1]["error"]

    assert updates[2]["success"] is False
    assert updates[2]["hg_error_class"] == InvalidPayloadError.__name__
    assert "[3250]" in updates[2]["error"]

    assert updates[4]["success"] is True
    assert updates[4]["id"] == "8000274D-1785964190"


def test_handle_batch_response_single_response_element(customers_sink: CustomersSink):
    """Handle xmlschema's single-element dict shape for one CustomerAddRs."""
    parsed = _load_parsed_responses("customer-batch-valid.response.xml")
    assert len(parsed) == 5

    staged = _staged_record(
        customers_sink,
        request_id=parsed[0]["request_id"],
        external_id="ext-ok-1",
        name="HG-BATCH-OK-1",
    )
    single_response = dict(parsed[0])
    result = customers_sink.handle_batch_response(
        {"items": [{"record": staged, "response": single_response}]}
    )
    update = result["state_updates"][0]

    assert update["success"] is True
    assert update["externalId"] == "ext-ok-1"
    assert update["id"] == "80002747-1785964187"


def test_handle_batch_response_missing_request_id(customers_sink: CustomersSink):
    """Return a failure state when no matching response exists for a staged record."""
    staged = _staged_record(
        customers_sink,
        request_id="missing-99",
        external_id="ext-missing",
        name="HG-MISSING",
    )

    result = customers_sink.handle_batch_response({"items": [{"record": staged, "response": None}]})
    update = result["state_updates"][0]

    assert update["success"] is False
    assert update["hg_error_class"] == InvalidPayloadError.__name__
    assert "No matching QuickBooks response" in update["error"]
    assert update["externalId"] == "ext-missing"
