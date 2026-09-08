"""Tests for cross-type item upsert lookup."""

from __future__ import annotations

from unittest.mock import patch

from target_qbwc.sinks import ItemNonInventorySink


def _inventory_item_ret(**fields) -> dict:
    """Build a minimal ItemInventoryRet fixture."""
    base = {
        "ListID": "8000002F-1786050464",
        "EditSequence": "1786050464",
        "Name": "HG-S4-INVIT-050363",
        "FullName": "HG-S4-INVIT-050363",
    }
    base.update(fields)
    return base


def test_item_noninventory_cross_type_match_skips_as_existing(
    item_noninventory_sink: ItemNonInventorySink,
):
    """Skip add when the name exists on a different item type."""
    staged = {
        "request_id": "0",
        "external_id": "item-1",
        "payload": {"Name": "HG-S4-INVIT-050363"},
    }
    write_request = item_noninventory_sink._build_write_request(
        staged,
        {
            "matches": [_inventory_item_ret()],
            "match_ret_types": ["ItemInventoryRet"],
            "query_failed": False,
        },
    )

    assert write_request["existing_skip"] is True
    assert write_request["resolved_entity_id"] == "8000002F-1786050464"


def test_item_noninventory_same_type_match_uses_mod(item_noninventory_sink: ItemNonInventorySink):
    """Mod non-inventory items when ItemQuery returns ItemNonInventoryRet."""
    staged = {
        "request_id": "0",
        "payload": {"Name": "10870-1", "SalesOrPurchase": {"AccountRef": {"FullName": "Sales"}}},
    }
    existing = {
        "ListID": "80000029-1785429724",
        "EditSequence": "1785429724",
        "Name": "10870-1",
        "FullName": "10870-1",
    }
    write_request = item_noninventory_sink._build_write_request(
        staged,
        {
            "matches": [existing],
            "match_ret_types": ["ItemNonInventoryRet"],
            "query_failed": False,
        },
    )

    assert write_request["write_op"] == "mod"
    assert write_request["resolved_entity_id"] == "80000029-1785429724"


def test_make_batch_request_item_cross_type_match_skips_write(
    item_noninventory_sink: ItemNonInventorySink,
):
    """Skip the write when ItemQuery finds the name on another item type."""
    staged = item_noninventory_sink.process_batch_record(
        {"externalId": "item-1", "Name": "HG-S4-INVIT-050363"},
        0,
    )
    query_response = {
        "ItemQueryRs": {
            "@requestID": "0",
            "@statusCode": "0",
            "ItemInventoryRet": _inventory_item_ret(),
        }
    }

    with patch.object(
        item_noninventory_sink,
        "send_qbxml_batch",
        return_value=query_response,
    ) as send_batch:
        result = item_noninventory_sink.make_batch_request([staged])

    assert send_batch.call_count == 1
    item = result["items"][0]
    assert item["existing_skip"] is True
    assert item["existing_id"] == "8000002F-1786050464"

    handled = item_noninventory_sink.handle_batch_response(result)
    update = handled["state_updates"][0]
    assert update["success"] is True
    assert update["id"] == "8000002F-1786050464"
    assert update.get("is_existing") is True
