"""UOM quantity rescaling for transaction line items."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterator

from hotglue_etl_exceptions import InvalidPayloadError

from target_qbwc.line_utils import normalize_line_list, normalize_unit_list

UOM_LINE_KEYS: dict[str, tuple[str, ...]] = {
    "invoice": ("InvoiceLineAdd", "InvoiceLineMod"),
    "bill": ("ItemLineAdd", "ItemLineMod"),
    "credit_memo": ("CreditMemoLineAdd", "CreditMemoLineMod"),
}

# Bill line externalIds are collected during bill preprocess; only strip on txn streams.
_UOM_STRIP_LINE_EXTERNAL_ID_STREAMS = frozenset({"invoice", "credit_memo"})


def iter_uom_lines(payload: dict[str, Any], stream: str) -> Iterator[dict[str, Any]]:
    """Yield item lines that have both Quantity and ItemRef for UOM rescaling."""
    for line_key in UOM_LINE_KEYS.get(stream, ()):
        for line in normalize_line_list(payload.get(line_key)):
            if not line.get("Quantity") or not line.get("ItemRef"):
                continue
            item_ref = line.get("ItemRef") or {}
            if not item_ref.get("ListID") and not item_ref.get("FullName"):
                continue
            yield line


def _default_uom_for_stream(stream: str, uom_set: dict[str, Any]) -> str | None:
    """Return the Sales or Purchase default unit name for one stream."""
    entity_group = "Sales" if stream in ("invoice", "credit_memo") else "Purchase"
    for default_unit in normalize_unit_list(uom_set.get("DefaultUnit")):
        if default_unit.get("UnitUsedFor") == entity_group:
            return default_unit.get("Unit")
    return None


def _valid_uom_names(uom_set: dict[str, Any]) -> list[str]:
    """Return base and related unit names for one UnitOfMeasureSet."""
    names: list[str] = []
    base_name = (uom_set.get("BaseUnit") or {}).get("Name")
    if base_name:
        names.append(base_name)
    for related_unit in normalize_unit_list(uom_set.get("RelatedUnit")):
        name = related_unit.get("Name")
        if name:
            names.append(name)
    return names


def fix_line_quantity_based_on_uom(
    line: dict[str, Any],
    stream: str,
    *,
    item: dict[str, Any] | None,
    uom_set: dict[str, Any] | None,
) -> InvalidPayloadError | None:
    """Rescale one line Quantity to QuickBooks base units when UOM data is available."""
    if not line.get("Quantity") or not line.get("ItemRef"):
        return None

    item_ref = line.get("ItemRef") or {}
    item_list_id = item_ref.get("ListID")
    item_full_name = item_ref.get("FullName")
    if not item_list_id and not item_full_name:
        return None

    if item is None:
        return None

    item_uom_set_ref_list_id = (item.get("UnitOfMeasureSetRef") or {}).get("ListID")
    if not item_uom_set_ref_list_id:
        return None

    if uom_set is None:
        return None

    related_units = normalize_unit_list(uom_set.get("RelatedUnit"))
    if not related_units:
        return None

    line_uom = line.get("UnitOfMeasure")
    if not line_uom:
        line_uom = _default_uom_for_stream(stream, uom_set)

    found_related_unit = next(
        (unit for unit in related_units if unit.get("Name") == line_uom),
        None,
    )
    if not found_related_unit:
        base_unit_name = (uom_set.get("BaseUnit") or {}).get("Name")
        if line_uom == base_unit_name:
            return None

        item_ref_label = item_full_name or item_list_id
        uom_set_name = uom_set.get("Name", "")
        valid_units = _valid_uom_names(uom_set)
        return InvalidPayloadError(
            f"Item '{item_ref_label}': UnitOfMeasure '{line_uom}' is not valid for "
            f"UOM set '{uom_set_name}'. Valid units are: {', '.join(valid_units)}."
        )

    conversion_ratio = found_related_unit.get("ConversionRatio", 1)
    try:
        quantity = Decimal(str(line["Quantity"])) * Decimal(str(conversion_ratio))
    except (InvalidOperation, ValueError, TypeError):
        item_ref_label = item_full_name or item_list_id
        return InvalidPayloadError(f"Item '{item_ref_label}': Quantity is not numeric.")
    line["Quantity"] = format(quantity, "f")
    return None


def _strip_txn_line_external_ids(payload: dict[str, Any], stream: str) -> None:
    """Remove Hotglue line externalId metadata before QBXML encoding."""
    for line_key in UOM_LINE_KEYS.get(stream, ()):
        for line in normalize_line_list(payload.get(line_key)):
            line.pop("externalId", None)


def rescale_payload_uom_quantities(
    payload: dict[str, Any],
    stream: str,
    *,
    lookup_item: Callable[[dict[str, Any]], dict[str, Any] | None],
    lookup_uom_set: Callable[[str], dict[str, Any] | None],
) -> InvalidPayloadError | None:
    """Rescale all affected line quantities on one transaction payload."""
    if stream in _UOM_STRIP_LINE_EXTERNAL_ID_STREAMS:
        _strip_txn_line_external_ids(payload, stream)
    for line in iter_uom_lines(payload, stream):
        item_ref = line.get("ItemRef") or {}
        item = lookup_item(item_ref)
        uom_set_list_id = (
            (item.get("UnitOfMeasureSetRef") or {}).get("ListID") if item else None
        )
        uom_set = lookup_uom_set(uom_set_list_id) if uom_set_list_id else None
        error = fix_line_quantity_based_on_uom(
            line,
            stream,
            item=item,
            uom_set=uom_set,
        )
        if error is not None:
            return error
    return None
