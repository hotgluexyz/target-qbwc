"""Shared fixtures for target-qbwc tests."""

import pytest

from target_qbwc.client import QbwcBatchSink
from target_qbwc.sinks import CustomersSink
from target_qbwc.target import TargetQbwc


class AddOnlyCustomerSink(QbwcBatchSink):
    """Minimal add-only sink for testing QbwcBatchSink without upsert."""

    name = "customer"
    qbxml_entity = "Customer"


@pytest.fixture
def target_config() -> dict:
    """Return a minimal target config for offline tests."""
    return {"token": "test-token", "is_sandbox": True}


@pytest.fixture
def customers_sink(target_config: dict) -> CustomersSink:
    """Return a Customers upsert sink wired to a minimal target config."""
    target = TargetQbwc(config=target_config)
    return CustomersSink(
        target=target,
        stream_name="customer",
        schema={"type": "object", "properties": {}},
        key_properties=["externalId"],
    )


@pytest.fixture
def add_only_customer_sink(target_config: dict) -> AddOnlyCustomerSink:
    """Return an add-only customer sink for testing QbwcBatchSink."""
    target = TargetQbwc(config=target_config)
    return AddOnlyCustomerSink(
        target=target,
        stream_name="customer",
        schema={"type": "object", "properties": {}},
        key_properties=["externalId"],
    )
