"""Offline tests for UOM quantity rescaling pure functions."""

from __future__ import annotations

from hotglue_etl_exceptions import InvalidPayloadError

from target_qbwc.tests.conftest import uom_item, uom_set
from target_qbwc.uom_quantities import (
    fix_line_quantity_based_on_uom,
    rescale_payload_uom_quantities,
)


def test_fix_line_skips_without_quantity_or_item_ref():
    """Skip rescaling when Quantity or ItemRef is missing."""
    line = {"Desc": "no quantity"}
    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set()) is None
    assert "Quantity" not in line

    line = {"Quantity": "5", "Desc": "no item"}
    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set()) is None


def test_fix_line_skips_when_item_or_uom_set_missing():
    """Skip rescaling when the item or UOM set cannot be resolved."""
    line = {"Quantity": "5", "ItemRef": {"FullName": "4080K"}}

    assert fix_line_quantity_based_on_uom(line, "invoice", item=None, uom_set=uom_set()) is None
    assert line["Quantity"] == "5"

    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=None) is None
    assert line["Quantity"] == "5"


def test_fix_line_skips_when_item_has_no_uom_set_ref():
    """Skip rescaling when the item has no UnitOfMeasureSetRef."""
    line = {"Quantity": "5", "ItemRef": {"FullName": "4080K"}}
    item = uom_item()
    item.pop("UnitOfMeasureSetRef")

    assert fix_line_quantity_based_on_uom(line, "invoice", item=item, uom_set=uom_set()) is None
    assert line["Quantity"] == "5"


def test_fix_line_skips_when_related_units_missing():
    """Skip rescaling when the UOM set has no related units."""
    line = {"Quantity": "5", "ItemRef": {"FullName": "4080K"}, "UnitOfMeasure": "case"}
    uom_set_fixture = uom_set()
    uom_set_fixture.pop("RelatedUnit")

    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set_fixture) is None
    assert line["Quantity"] == "5"


def test_fix_line_converts_non_base_unit():
    """Multiply quantity by ConversionRatio for a non-base related unit."""
    line = {
        "Quantity": "5",
        "ItemRef": {"FullName": "4080K"},
        "UnitOfMeasure": "case",
    }

    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set()) is None
    assert line["Quantity"] == "50"


def test_fix_line_noop_for_base_unit():
    """Leave quantity unchanged when UnitOfMeasure matches the base unit."""
    line = {
        "Quantity": "5",
        "ItemRef": {"FullName": "4080K"},
        "UnitOfMeasure": "each",
    }

    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set()) is None
    assert line["Quantity"] == "5"


def test_fix_line_uses_sales_default_when_uom_omitted_on_invoice():
    """Default to the Sales unit when UnitOfMeasure is omitted on invoice lines."""
    line = {
        "Quantity": "3",
        "ItemRef": {"FullName": "4080K"},
    }
    uom_set_fixture = uom_set(
        DefaultUnit=[
            {"UnitUsedFor": "Purchase", "Unit": "case"},
            {"UnitUsedFor": "Sales", "Unit": "pack"},
        ]
    )

    assert fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set_fixture) is None
    assert line["Quantity"] == "15"


def test_fix_line_uses_purchase_default_on_bill():
    """Default to the Purchase unit when UnitOfMeasure is omitted on bill lines."""
    line = {
        "Quantity": "2",
        "ItemRef": {"FullName": "4080K"},
    }
    uom_set_fixture = uom_set(
        DefaultUnit=[
            {"UnitUsedFor": "Purchase", "Unit": "case"},
            {"UnitUsedFor": "Sales", "Unit": "pack"},
        ]
    )

    assert fix_line_quantity_based_on_uom(line, "bill", item=uom_item(), uom_set=uom_set_fixture) is None
    assert line["Quantity"] == "20"


def test_fix_line_invalid_uom_returns_payload_error():
    """Return InvalidPayloadError with valid unit names when UOM is unknown."""
    line = {
        "Quantity": "5",
        "ItemRef": {"FullName": "4080K"},
        "UnitOfMeasure": "pallet",
    }

    error = fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set())

    assert isinstance(error, InvalidPayloadError)
    assert "UnitOfMeasure 'pallet' is not valid" in str(error)
    assert "each, case, pack" in str(error)
    assert line["Quantity"] == "5"


def test_fix_line_non_numeric_quantity_returns_payload_error():
    """Return InvalidPayloadError when Quantity cannot be parsed as a number."""
    line = {
        "Quantity": "not-a-number",
        "ItemRef": {"FullName": "4080K"},
        "UnitOfMeasure": "case",
    }

    error = fix_line_quantity_based_on_uom(line, "invoice", item=uom_item(), uom_set=uom_set())

    assert isinstance(error, InvalidPayloadError)
    assert "Quantity is not numeric" in str(error)
    assert line["Quantity"] == "not-a-number"


def test_rescale_payload_processes_all_invoice_lines():
    """Rescale every affected line key on one payload."""
    payload = {
        "InvoiceLineAdd": [
            {"Quantity": "5", "ItemRef": {"FullName": "4080K"}, "UnitOfMeasure": "case"},
            {"Quantity": "2", "ItemRef": {"FullName": "4080K"}, "UnitOfMeasure": "pack"},
        ]
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    def lookup_item(item_ref: dict) -> dict | None:
        return item

    def lookup_uom_set(list_id: str) -> dict | None:
        assert list_id == "UOM-001"
        return uom_set_fixture

    assert (
        rescale_payload_uom_quantities(
            payload,
            "invoice",
            lookup_item=lookup_item,
            lookup_uom_set=lookup_uom_set,
        )
        is None
    )
    assert payload["InvoiceLineAdd"][0]["Quantity"] == "50"
    assert payload["InvoiceLineAdd"][1]["Quantity"] == "10"


def test_rescale_payload_processes_invoice_line_mod():
    """Rescale InvoiceLineMod quantities the same as add lines."""
    payload = {
        "InvoiceLineMod": [
            {
                "TxnLineID": "1",
                "Quantity": "2",
                "ItemRef": {"FullName": "4080K"},
                "UnitOfMeasure": "pack",
            }
        ]
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    assert (
        rescale_payload_uom_quantities(
            payload,
            "invoice",
            lookup_item=lambda _: item,
            lookup_uom_set=lambda _: uom_set_fixture,
        )
        is None
    )
    assert payload["InvoiceLineMod"][0]["Quantity"] == "10"


def test_rescale_payload_processes_credit_memo_line_mod():
    """Rescale CreditMemoLineMod quantities the same as add lines."""
    payload = {
        "CreditMemoLineMod": [
            {
                "TxnLineID": "1",
                "Quantity": "3",
                "ItemRef": {"FullName": "4080K"},
                "UnitOfMeasure": "case",
            }
        ]
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    assert (
        rescale_payload_uom_quantities(
            payload,
            "credit_memo",
            lookup_item=lambda _: item,
            lookup_uom_set=lambda _: uom_set_fixture,
        )
        is None
    )
    assert payload["CreditMemoLineMod"][0]["Quantity"] == "30"


def test_rescale_payload_strips_invoice_line_external_id():
    """Strip line externalId on invoice payloads before QBXML encoding."""
    payload = {
        "InvoiceLineAdd": [
            {
                "Quantity": "1",
                "ItemRef": {"FullName": "4080K"},
                "UnitOfMeasure": "each",
                "externalId": "line-1",
            }
        ]
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    assert (
        rescale_payload_uom_quantities(
            payload,
            "invoice",
            lookup_item=lambda _: item,
            lookup_uom_set=lambda _: uom_set_fixture,
        )
        is None
    )
    assert "externalId" not in payload["InvoiceLineAdd"][0]


def test_rescale_payload_preserves_bill_line_external_id():
    """Keep bill line externalIds for bill sink customData collection."""
    payload = {
        "ItemLineAdd": [
            {
                "Quantity": "2",
                "ItemRef": {"FullName": "4080K"},
                "UnitOfMeasure": "case",
                "externalId": "item-line-1",
            }
        ]
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    assert (
        rescale_payload_uom_quantities(
            payload,
            "bill",
            lookup_item=lambda _: item,
            lookup_uom_set=lambda _: uom_set_fixture,
        )
        is None
    )
    assert payload["ItemLineAdd"][0]["externalId"] == "item-line-1"
    assert payload["ItemLineAdd"][0]["Quantity"] == "20"


def test_rescale_payload_processes_item_line_mod_on_bill():
    """Rescale ItemLineMod quantities and preserve line externalIds on bills."""
    payload = {
        "ItemLineMod": [
            {
                "TxnLineID": "1",
                "Quantity": "2",
                "ItemRef": {"FullName": "4080K"},
                "UnitOfMeasure": "pack",
                "externalId": "item-mod-1",
            }
        ]
    }
    item = uom_item()
    uom_set_fixture = uom_set()

    assert (
        rescale_payload_uom_quantities(
            payload,
            "bill",
            lookup_item=lambda _: item,
            lookup_uom_set=lambda _: uom_set_fixture,
        )
        is None
    )
    assert payload["ItemLineMod"][0]["Quantity"] == "10"
    assert payload["ItemLineMod"][0]["externalId"] == "item-mod-1"
