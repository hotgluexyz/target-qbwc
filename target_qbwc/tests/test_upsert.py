"""Offline tests for QbwcUpsertBatchSink lookup, merge and write staging."""

from __future__ import annotations

import logging
from unittest.mock import patch

from hotglue_etl_exceptions import InvalidPayloadError
from hotglue_singer_sdk.exceptions import FatalAPIError

from target_qbwc.client_upsert import LOOKUP_QUERY_FAILED_MESSAGE
from target_qbwc.sinks import CustomersSink


def _customer_ret(**fields) -> dict:
    """Build a minimal CustomerRet fixture."""
    base = {
        "ListID": "80002754-1786031476",
        "TimeCreated": "2026-08-05T16:43:30-04:00",
        "TimeModified": "2026-08-05T16:43:30-04:00",
        "EditSequence": "1786031476",
        "Name": "HG-TGT-E2E-001",
        "FullName": "HG-TGT-E2E-001",
        "Sublevel": "0",
        "Balance": "0.00",
        "TotalBalance": "0.00",
        "CompanyName": "Old Company",
        "Phone": "555-0101",
    }
    base.update(fields)
    return base


def test_merge_for_mod_overlays_payload_and_keeps_identity(customers_sink: CustomersSink):
    """Overlay incoming fields onto the queried record and keep ListID and EditSequence."""
    merged = customers_sink._merge_for_mod(
        _customer_ret(),
        {"CompanyName": "Updated Company", "Phone": "555-9999"},
    )

    assert merged["ListID"] == "80002754-1786031476"
    assert merged["EditSequence"] == "1786031476"
    assert merged["CompanyName"] == "Updated Company"
    assert merged["Phone"] == "555-9999"
    assert merged["Name"] == "HG-TGT-E2E-001"
    assert "Balance" not in merged
    assert "FullName" not in merged


def test_merge_for_mod_strips_ret_only_fields_from_existing_not_incoming(customers_sink: CustomersSink):
    """Strip Ret-only keys from the query result but keep them when the user payload sends them."""
    merged = customers_sink._merge_for_mod(
        _customer_ret(),
        {"Balance": "99.00", "CompanyName": "Updated Company"},
    )

    assert merged["Balance"] == "99.00"
    assert merged["CompanyName"] == "Updated Company"
    assert "FullName" not in merged
    assert "TimeCreated" not in merged


def test_build_write_request_mod_rejects_unknown_incoming_field(customers_sink: CustomersSink):
    """Reject mod writes when incoming payload includes a field not defined on CustomerMod."""
    staged = {
        "request_id": "0",
        "payload": {"Name": "HG-TGT-E2E-001", "Balance": "99.00"},
    }
    write_request = customers_sink._build_write_request(
        staged,
        {"matches": [_customer_ret()], "query_failed": False},
    )

    assert write_request["write_op"] == "mod"
    assert write_request["preprocess_error"] is not None
    message = str(write_request["preprocess_error"]).lower()
    assert "balance" in message
    assert "unknown field" in message


def test_build_write_request_mod_rejects_ret_shaped_incoming_field(customers_sink: CustomersSink):
    """Reject mod writes when incoming payload uses a Ret-only field name such as FullName."""
    staged = {
        "request_id": "0",
        "payload": {"Name": "HG-TGT-E2E-001", "FullName": "HG-TGT-E2E-001"},
    }
    write_request = customers_sink._build_write_request(
        staged,
        {"matches": [_customer_ret()], "query_failed": False},
    )

    assert write_request["preprocess_error"] is not None
    message = str(write_request["preprocess_error"]).lower()
    assert "fullname" in message
    assert "unknown field" in message


def test_build_write_request_uses_mod_for_single_match(customers_sink: CustomersSink):
    """Build CustomerModRq when lookup returns exactly one customer."""
    staged = {
        "request_id": "0",
        "payload": {"Name": "HG-TGT-E2E-001", "CompanyName": "Updated Company"},
    }
    write_request = customers_sink._build_write_request(
        staged,
        {"matches": [_customer_ret()], "query_failed": False},
    )

    assert write_request["write_op"] == "mod"
    assert write_request["write_response_element"] == "CustomerModRs"
    assert write_request["request_element"] == {
        "CustomerModRq": {
            "@requestID": "0",
            "CustomerMod": customers_sink._merge_for_mod(
                _customer_ret(),
                staged["payload"],
            ),
        },
    }
    assert write_request["preprocess_error"] is None


def test_build_write_request_uses_add_for_no_matches(customers_sink: CustomersSink):
    """Build CustomerAddRq when lookup returns zero customers."""
    staged = {
        "request_id": "1",
        "payload": {"Name": "HG-NEW-001", "CompanyName": "New Co"},
    }
    write_request = customers_sink._build_write_request(
        staged,
        {"matches": [], "query_failed": False},
    )

    assert write_request["write_op"] == "add"
    assert write_request["request_element"] == {
        "CustomerAddRq": {
            "@requestID": "1",
            "CustomerAdd": staged["payload"],
        },
    }


def test_build_write_request_skips_write_when_lookup_failed(customers_sink: CustomersSink):
    """Skip add/mod when the lookup query failed."""
    staged = {
        "request_id": "1",
        "payload": {"Name": "HG-NEW-001", "CompanyName": "New Co"},
    }
    write_request = customers_sink._build_write_request(
        staged,
        {"matches": [], "query_failed": True},
    )

    assert "lookup_error" in write_request
    assert "write_op" not in write_request
    assert str(write_request["lookup_error"]) == LOOKUP_QUERY_FAILED_MESSAGE


def test_build_write_request_returns_ambiguous_error(customers_sink: CustomersSink):
    """Fail the record when lookup returns multiple customers."""
    staged = {
        "request_id": "2",
        "payload": {"Name": "HG-TGT-E2E-001", "CompanyName": "Dup Co"},
    }
    write_request = customers_sink._build_write_request(
        staged,
        {"matches": [_customer_ret(), _customer_ret(ListID="80002755-1786031476")], "query_failed": False},
    )

    assert "ambiguous_error" in write_request
    message = str(write_request["ambiguous_error"])
    assert "multiple existing records" in message
    assert "FullName" in message


def test_interpret_query_response_collects_all_matches(customers_sink: CustomersSink):
    """Parse every CustomerRet from a query response."""
    outcome = customers_sink._interpret_query_response(
        {
            "@requestID": "0",
            "@statusCode": "0",
            "@statusSeverity": "Info",
            "CustomerRet": [
                _customer_ret(),
                _customer_ret(ListID="80002755-1786031476"),
            ],
        }
    )

    assert outcome["query_failed"] is False
    assert len(outcome["matches"]) == 2


def test_interpret_query_response_treats_not_found_as_zero_matches(customers_sink: CustomersSink):
    """Treat QuickBooks not-found query status as zero matches, not lookup failure."""
    outcome = customers_sink._interpret_query_response(
        {
            "@requestID": "0",
            "@statusCode": 500,
            "@statusSeverity": "Warn",
            "@statusMessage": (
                "The query request has not been fully completed. There was a required element "
                "(\"NONEXISTENT\") that could not be found in QuickBooks."
            ),
        }
    )

    assert outcome["matches"] == []
    assert outcome["query_failed"] is False


def test_interpret_query_response_marks_transport_failure(customers_sink: CustomersSink):
    """Treat non-zero query status as lookup failure."""
    outcome = customers_sink._interpret_query_response(
        {
            "@requestID": "0",
            "@statusCode": "500",
            "@statusSeverity": "Error",
            "@statusMessage": "Query failed",
        }
    )

    assert outcome["matches"] == []
    assert outcome["query_failed"] is True


def test_handle_batch_response_marks_mod_success_as_updated(customers_sink: CustomersSink):
    """Set is_updated on successful mod responses."""
    payload = {"Name": "HG-TGT-E2E-001", "CompanyName": "Updated Company"}
    staged = {
        "request_id": "0",
        "external_id": "cust-update",
        "payload": payload,
        "write_op": "mod",
    }
    result = customers_sink.handle_batch_response(
        {
            "items": [
                {
                    "record": staged,
                    "response": {
                        "request_id": "0",
                        "status_code": "0",
                        "status_message": "Status OK",
                        "entity": {"ListID": "80002754-1786031476"},
                    },
                }
            ]
        }
    )
    update = result["state_updates"][0]

    assert update["success"] is True
    assert update["is_updated"] is True
    assert update["id"] == "80002754-1786031476"
    assert update["externalId"] == "cust-update"


def test_handle_batch_response_ambiguous_match(customers_sink: CustomersSink):
    """Map ambiguous lookup matches to InvalidPayloadError state."""
    payload = {"Name": "HG-TGT-E2E-001", "CompanyName": "Dup Co"}
    staged = {
        "request_id": "0",
        "external_id": "cust-dup",
        "payload": payload,
    }
    result = customers_sink.handle_batch_response(
        {
            "items": [
                {
                    "record": staged,
                    "ambiguous_error": customers_sink._build_ambiguous_match_error(payload),
                }
            ]
        }
    )
    update = result["state_updates"][0]

    assert update["success"] is False
    assert update["hg_error_class"] == InvalidPayloadError.__name__
    assert "multiple existing records" in update["error"]


def test_make_batch_request_runs_lookup_then_add(customers_sink: CustomersSink):
    """Run a batched lookup with no match, then a batched add write."""
    staged = customers_sink.process_batch_record(
        {"externalId": "new-cust", "Name": "HG-NEW-001", "CompanyName": "New Co"},
        0,
    )
    query_response = {
        "CustomerQueryRs": {
            "@requestID": "0",
            "@statusCode": "500",
            "@statusMessage": (
                'The query request has not been fully completed. There was a required element '
                '("HG-NEW-001") that could not be found in QuickBooks.'
            ),
        }
    }
    write_response = {
        "CustomerAddRs": {
            "@requestID": "0",
            "@statusCode": "0",
            "@statusMessage": "Status OK",
            "CustomerRet": {"ListID": "80002799-1786041000"},
        }
    }

    with patch.object(
        customers_sink,
        "send_qbxml_batch",
        side_effect=[query_response, write_response],
    ) as send_batch:
        result = customers_sink.make_batch_request([staged])

    assert send_batch.call_count == 2
    assert len(result["items"]) == 1
    item = result["items"][0]
    assert item["record"]["write_op"] == "add"
    assert item["response"]["status_code"] == "0"
    assert item["response"]["entity"]["ListID"] == "80002799-1786041000"


def test_make_batch_request_runs_lookup_then_mod(customers_sink: CustomersSink):
    """Run a batched lookup with one match, then a batched mod write."""
    staged = customers_sink.process_batch_record(
        {
            "externalId": "update-cust",
            "Name": "HG-TGT-E2E-001",
            "CompanyName": "Updated Company",
        },
        0,
    )
    query_response = {
        "CustomerQueryRs": {
            "@requestID": "0",
            "@statusCode": "0",
            "CustomerRet": _customer_ret(),
        }
    }
    write_response = {
        "CustomerModRs": {
            "@requestID": "0",
            "@statusCode": "0",
            "@statusMessage": "Status OK",
            "CustomerRet": {"ListID": "80002754-1786031476"},
        }
    }

    with patch.object(
        customers_sink,
        "send_qbxml_batch",
        side_effect=[query_response, write_response],
    ) as send_batch:
        result = customers_sink.make_batch_request([staged])

    assert send_batch.call_count == 2
    item = result["items"][0]
    assert item["record"]["write_op"] == "mod"
    assert item["response"]["status_code"] == "0"


def test_make_batch_request_query_batch_failure_skips_write(customers_sink: CustomersSink):
    """Skip the write when the lookup batch fails in transport."""
    staged = customers_sink.process_batch_record(
        {"externalId": "fallback-cust", "Name": "HG-FALLBACK-001", "CompanyName": "Fallback Co"},
        0,
    )

    with patch.object(
        customers_sink,
        "send_qbxml_batch",
        side_effect=RuntimeError("lookup transport failed"),
    ) as send_batch:
        result = customers_sink.make_batch_request([staged])

    assert send_batch.call_count == 1
    item = result["items"][0]
    assert isinstance(item["lookup_error"], FatalAPIError)
    assert "lookup transport failed" in str(item["lookup_error"])
    assert "write_op" not in item["record"]

    handled = customers_sink.handle_batch_response(result)
    update = handled["state_updates"][0]
    assert update["success"] is False
    assert "hg_error_class" not in update
    assert "lookup transport failed" in update["error"]
    assert update["externalId"] == "fallback-cust"


def test_make_batch_request_query_rs_failure_skips_write(customers_sink: CustomersSink):
    """Skip the write when a lookup *Rs is not a not-found miss."""
    staged = customers_sink.process_batch_record(
        {"externalId": "query-fail-cust", "Name": "HG-QUERY-FAIL-001", "CompanyName": "Fail Co"},
        0,
    )
    query_response = {
        "CustomerQueryRs": {
            "@requestID": "0",
            "@statusCode": "500",
            "@statusSeverity": "Error",
            "@statusMessage": "Query failed",
        }
    }

    with patch.object(
        customers_sink,
        "send_qbxml_batch",
        return_value=query_response,
    ) as send_batch:
        result = customers_sink.make_batch_request([staged])

    assert send_batch.call_count == 1
    item = result["items"][0]
    assert str(item["lookup_error"]) == LOOKUP_QUERY_FAILED_MESSAGE
    assert "write_op" not in item["record"]

    handled = customers_sink.handle_batch_response(result)
    update = handled["state_updates"][0]
    assert update["success"] is False
    assert "hg_error_class" not in update
    assert update["error"] == LOOKUP_QUERY_FAILED_MESSAGE


def test_log_write_decision_mod(customers_sink: CustomersSink, caplog):
    """Log mod decisions with lookup key, matched id and externalId when present."""
    with caplog.at_level(logging.INFO, logger=customers_sink.logger.name):
        customers_sink._log_write_decision(
            {
                "external_id": "cust-update",
                "payload": {"Name": "HG-TGT-E2E-001", "CompanyName": "Updated Company"},
            },
            "mod",
            "80002754-1786031476",
        )

    assert caplog.records[-1].message == (
        "customer lookup matched FullName=HG-TGT-E2E-001, id: 80002754-1786031476, "
        "externalId: cust-update, op: mod"
    )


def test_log_write_decision_add_no_match(customers_sink: CustomersSink, caplog):
    """Log add decisions with lookup key when query criteria were present but unmatched."""
    with caplog.at_level(logging.INFO, logger=customers_sink.logger.name):
        customers_sink._log_write_decision(
            {"payload": {"Name": "HG-NEW-001"}},
            "add",
        )

    assert caplog.records[-1].message == "customer lookup no match FullName=HG-NEW-001, op: add"


def test_log_write_decision_add_no_lookup_field(customers_sink: CustomersSink, caplog):
    """Log add decisions when the payload has no lookup fields."""
    with caplog.at_level(logging.INFO, logger=customers_sink.logger.name):
        customers_sink._log_write_decision(
            {
                "external_id": "cust-add",
                "payload": {"CompanyName": "New Co"},
            },
            "add",
        )

    assert caplog.records[-1].message == (
        "customer no lookup field, externalId: cust-add, op: add"
    )


def test_build_lookup_query_element_stitches_parent_full_name(customers_sink: CustomersSink):
    """Lookup sub-customers using ParentRef.FullName:Name as QBD FullName."""
    query_element = customers_sink._build_lookup_query_element(
        {
            "Name": "SpaceX",
            "ParentRef": {"FullName": "Elon Musk"},
        },
        "0",
    )

    assert query_element == {
        "CustomerQueryRq": {
            "@requestID": "0",
            "FullName": "Elon Musk:SpaceX",
        }
    }


def test_build_lookup_query_element_stitches_parent_list_id(customers_sink: CustomersSink):
    """Resolve ParentRef.ListID to parent FullName before lookup."""
    with patch.object(
        customers_sink,
        "_query_customer_full_name_by_list_id",
        return_value="Elon Musk",
    ):
        query_element = customers_sink._build_lookup_query_element(
            {
                "Name": "SpaceX",
                "ParentRef": {"ListID": "80000009-1750961692"},
            },
            "1",
        )

    assert query_element == {
        "CustomerQueryRq": {
            "@requestID": "1",
            "FullName": "Elon Musk:SpaceX",
        }
    }


def test_build_lookup_query_element_uses_list_id_when_present(customers_sink: CustomersSink):
    """Prefer direct ListID lookup when payload includes ListID."""
    query_element = customers_sink._build_lookup_query_element(
        {
            "ListID": "80000009-1750961692",
            "Name": "SpaceX",
            "ParentRef": {"FullName": "Elon Musk"},
        },
        "2",
    )

    assert query_element == {
        "CustomerQueryRq": {
            "@requestID": "2",
            "ListID": "80000009-1750961692",
        }
    }

