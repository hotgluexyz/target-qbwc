"""Shared ItemQueryRq parsing helpers."""

from __future__ import annotations

from typing import Any

ITEM_QUERY_RET_KEYS = (
    "ItemServiceRet",
    "ItemNonInventoryRet",
    "ItemOtherChargeRet",
    "ItemInventoryRet",
    "ItemInventoryAssemblyRet",
    "ItemFixedAssetRet",
    "ItemSubtotalRet",
    "ItemDiscountRet",
    "ItemPaymentRet",
    "ItemSalesTaxRet",
    "ItemSalesTaxGroupRet",
    "ItemGroupRet",
)


def extract_typed_item_matches(
    rs_element: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """Return (ret_element_name, entity) pairs from one ItemQueryRs element."""
    matches: list[tuple[str, dict[str, Any]]] = []
    for key in ITEM_QUERY_RET_KEYS:
        ret = rs_element.get(key)
        if ret is None:
            continue
        if isinstance(ret, list):
            matches.extend((key, entity) for entity in ret)
        else:
            matches.append((key, ret))
    return matches


def extract_item_query_results(rs_element: dict[str, Any]) -> list[dict[str, Any]]:
    """Return all item *Ret entities from one ItemQueryRs element."""
    return [entity for _, entity in extract_typed_item_matches(rs_element)]
