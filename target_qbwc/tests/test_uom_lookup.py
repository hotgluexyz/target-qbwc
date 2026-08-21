"""Offline tests for UomLookupCache."""

from __future__ import annotations

from target_qbwc.tests.conftest import uom_item, uom_set
from target_qbwc.uom_lookup import ItemRefKey, UomLookupCache


def test_uom_lookup_cache_indexes_and_misses_items():
    """Cache items by ListID and FullName and record negative lookups."""
    cache = UomLookupCache()
    item = uom_item()

    assert not cache.has_item_ref({"FullName": "4080K"})
    cache.store_item(item)
    assert cache.has_item_ref({"ListID": "ITEM-001"})
    assert cache.has_item_ref({"FullName": "4080K"})
    assert cache.lookup_item({"FullName": "4080K"}) == item

    cache.mark_item_missing(ItemRefKey(list_id="MISSING", full_name="missing-item"))
    assert cache.has_item_ref({"ListID": "MISSING"})
    assert cache.lookup_item({"ListID": "MISSING"}) is None


def test_uom_lookup_cache_indexes_uom_sets():
    """Cache UnitOfMeasureSet lookups by ListID."""
    cache = UomLookupCache()
    uom_set_fixture = uom_set()

    assert not cache.has_uom_set("UOM-001")
    cache.store_uom_set(uom_set_fixture)
    assert cache.has_uom_set("UOM-001")
    assert cache.lookup_uom_set("UOM-001") == uom_set_fixture

    cache.mark_uom_set_missing("UOM-MISSING")
    assert cache.has_uom_set("UOM-MISSING")
    assert cache.lookup_uom_set("UOM-MISSING") is None
