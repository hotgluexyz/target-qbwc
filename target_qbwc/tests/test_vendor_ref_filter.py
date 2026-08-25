"""Tests for vendor-scoped RefNumber lookup filtering."""

from __future__ import annotations

import pytest

from target_qbwc.client_upsert import filter_matches_by_vendor_ref
from target_qbwc.sinks import BillsSink, PurchaseOrdersSink
from target_qbwc.tests.conftest import make_sink

TXN_LOOKUP_FIELDS = [("TxnID", "TxnID"), ("RefNumber", "RefNumber")]

MATCHES = [
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


def test_filter_by_vendor_ref_for_refnumber_lookup():
    """RefNumber lookups are scoped by VendorRef."""
    filtered = filter_matches_by_vendor_ref(
        MATCHES,
        {"RefNumber": "BILL-001", "VendorRef": {"ListID": "V2"}},
        TXN_LOOKUP_FIELDS,
    )

    assert len(filtered) == 1
    assert filtered[0]["TxnID"] == "2"


def test_bypass_vendor_filter_for_txnid_lookup():
    """TxnID lookups keep matches even when VendorRef differs or is missing."""
    filtered = filter_matches_by_vendor_ref(
        [MATCHES[0]],
        {"TxnID": "1", "RefNumber": "BILL-001", "VendorRef": {"ListID": "V2"}},
        TXN_LOOKUP_FIELDS,
    )

    assert filtered == [MATCHES[0]]


def test_bypass_vendor_filter_when_txnid_lookup_has_no_vendor_ref():
    """TxnID lookup without VendorRef preserves the matched record."""
    filtered = filter_matches_by_vendor_ref(
        [MATCHES[0]],
        {"TxnID": "1"},
        TXN_LOOKUP_FIELDS,
    )

    assert filtered == [MATCHES[0]]


def test_refnumber_lookup_without_vendor_ref_returns_all_matches():
    """RefNumber lookup without VendorRef does not post-filter."""
    filtered = filter_matches_by_vendor_ref(
        MATCHES,
        {"RefNumber": "BILL-001"},
        TXN_LOOKUP_FIELDS,
    )

    assert filtered == MATCHES


@pytest.mark.parametrize("sink_cls", [BillsSink, PurchaseOrdersSink])
def test_sink_delegates_to_shared_vendor_ref_filter(sink_cls, target_config):
    """Bill and purchase order sinks use the shared vendor filter helper."""
    sink = make_sink(sink_cls, target_config)

    filtered = sink._filter_query_matches(
        MATCHES,
        {"RefNumber": "PO-001", "VendorRef": {"FullName": "Vendor B"}},
    )

    assert len(filtered) == 1
    assert filtered[0]["TxnID"] == "2"


@pytest.mark.parametrize("sink_cls", [BillsSink, PurchaseOrdersSink])
def test_sink_bypasses_vendor_filter_for_txnid_lookup(sink_cls, target_config):
    """Bill and purchase order sinks skip vendor filtering on TxnID lookup."""
    sink = make_sink(sink_cls, target_config)

    filtered = sink._filter_query_matches(
        [MATCHES[0]],
        {"TxnID": "1", "VendorRef": {"ListID": "V2"}},
    )

    assert filtered == [MATCHES[0]]
