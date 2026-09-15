"""Shared fixtures for target-qbwc tests."""

from __future__ import annotations

import pytest

from target_qbwc.client import QbwcBatchSink
from target_qbwc.sinks import BillsSink, CustomersSink, InvoicesSink, ItemNonInventorySink
from target_qbwc.target import TargetQbwc


class AddOnlyCustomerSink(QbwcBatchSink):
    """Minimal add-only sink for testing QbwcBatchSink without upsert."""

    name = "customer"
    qbxml_entity = "Customer"


def make_sink(sink_cls, target_config: dict):
    """Instantiate a sink wired to a minimal target."""
    target = TargetQbwc(config=target_config)
    return sink_cls(
        target=target,
        stream_name=sink_cls.name,
        schema={"type": "object", "properties": {}},
        key_properties=["externalId"],
    )


def uom_set(**overrides) -> dict:
    """Build a minimal UnitOfMeasureSetRet fixture."""
    base = {
        "ListID": "UOM-001",
        "Name": "1/10 Count in each",
        "BaseUnit": {"Name": "each", "Abbreviation": "ea"},
        "RelatedUnit": [
            {"Name": "case", "Abbreviation": "cs", "ConversionRatio": "10"},
            {"Name": "pack", "Abbreviation": "pk", "ConversionRatio": "5"},
        ],
        "DefaultUnit": [
            {"UnitUsedFor": "Purchase", "Unit": "each"},
            {"UnitUsedFor": "Sales", "Unit": "each"},
        ],
    }
    base.update(overrides)
    return base


def uom_item(**overrides) -> dict:
    """Build a minimal item fixture with a UOM set reference."""
    base = {
        "ListID": "ITEM-001",
        "FullName": "4080K",
        "UnitOfMeasureSetRef": {"ListID": "UOM-001", "FullName": "1/10 Count in each"},
    }
    base.update(overrides)
    return base


@pytest.fixture
def target_config() -> dict:
    """Return a minimal target config for offline tests."""
    return {"token": "test-token", "is_sandbox": True}


@pytest.fixture
def customers_sink(target_config: dict) -> CustomersSink:
    """Return a Customers upsert sink wired to a minimal target config."""
    return make_sink(CustomersSink, target_config)


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


@pytest.fixture
def bills_sink(target_config: dict) -> BillsSink:
    """Return a Bills upsert sink wired to a minimal target config."""
    return make_sink(BillsSink, target_config)


@pytest.fixture
def invoices_sink(target_config: dict) -> InvoicesSink:
    """Return an Invoices upsert sink wired to a minimal target config."""
    return make_sink(InvoicesSink, target_config)


@pytest.fixture
def item_noninventory_sink(target_config: dict) -> ItemNonInventorySink:
    """Return an ItemNonInventory upsert sink wired to a minimal target config."""
    return make_sink(ItemNonInventorySink, target_config)
