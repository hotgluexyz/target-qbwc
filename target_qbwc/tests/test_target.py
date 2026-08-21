"""Tests for TargetQbwc transport wiring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from target_qbwc.sinks import CustomersSink, VendorsSink
from target_qbwc.target import TargetQbwc


def test_stream_sinks_share_target_qbwc_client(target_config: dict):
    """All sinks on one target reuse a single QBWC client and session."""
    target = TargetQbwc(config=target_config)
    customers_sink = CustomersSink(
        target=target,
        stream_name="customer",
        schema={"type": "object", "properties": {}},
        key_properties=["externalId"],
    )
    vendors_sink = VendorsSink(
        target=target,
        stream_name="vendor",
        schema={"type": "object", "properties": {}},
        key_properties=["externalId"],
    )

    mock_client = MagicMock()
    with patch("target_qbwc.target.QBWCClient", return_value=mock_client) as mock_ctor:
        first = customers_sink.qbwc_client
        second = vendors_sink.qbwc_client

    assert mock_ctor.call_count == 1
    mock_client.create_session.assert_called_once()
    assert first is second is mock_client
