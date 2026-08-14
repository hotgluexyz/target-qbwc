"""UOM item and UnitOfMeasureSet lookup cache and batch prefetch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hotglue_etl_exceptions import InvalidPayloadError

from qbwc_common import normalize_rs_list

from target_qbwc.uom_quantities import iter_uom_lines, rescale_payload_uom_quantities

_ITEM_QUERY_RET_KEYS = (
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


@dataclass(frozen=True)
class ItemRefKey:
    """Normalized lookup key for one ItemRef."""

    list_id: str | None
    full_name: str | None


def item_ref_key(item_ref: dict[str, Any]) -> ItemRefKey | None:
    """Return a normalized ItemRef key when ListID or FullName is present."""
    list_id = item_ref.get("ListID")
    full_name = item_ref.get("FullName")
    if not list_id and not full_name:
        return None
    return ItemRefKey(list_id=list_id, full_name=full_name)


def _extract_item_query_results(rs_element: dict[str, Any]) -> list[dict[str, Any]]:
    """Return all item *Ret entities from one ItemQueryRs element."""
    entities: list[dict[str, Any]] = []
    for key in _ITEM_QUERY_RET_KEYS:
        ret = rs_element.get(key)
        if ret is None:
            continue
        if isinstance(ret, list):
            entities.extend(ret)
        else:
            entities.append(ret)
    return entities


def _interpret_query_rs(
    rs_element: dict[str, Any] | None,
    ret_element_name: str,
) -> list[dict[str, Any]]:
    """Parse a query *Rs element into a list of *Ret entities."""
    if rs_element is None:
        return []

    raw_status = rs_element.get("@statusCode")
    status_code = int(raw_status) if raw_status is not None else -1
    if status_code != 0:
        return []

    if ret_element_name == "ItemRet":
        return _extract_item_query_results(rs_element)

    ret = rs_element.get(ret_element_name)
    if ret is None:
        return []
    if isinstance(ret, list):
        return ret
    return [ret]


class UomLookupCache:
    """Target-scoped read-through cache for item and UnitOfMeasureSet lookups."""

    def __init__(self) -> None:
        self._items_by_list_id: dict[str, dict[str, Any] | None] = {}
        self._items_by_full_name: dict[str, dict[str, Any] | None] = {}
        self._uom_sets: dict[str, dict[str, Any] | None] = {}

    def has_item_ref(self, item_ref: dict[str, Any]) -> bool:
        """Return whether an ItemRef is already cached."""
        key = item_ref_key(item_ref)
        if key is None:
            return True
        if key.list_id and key.list_id in self._items_by_list_id:
            return True
        if key.full_name and key.full_name.lower() in self._items_by_full_name:
            return True
        return False

    def lookup_item(self, item_ref: dict[str, Any]) -> dict[str, Any] | None:
        """Return a cached item for an ItemRef, or None when unknown or missing."""
        key = item_ref_key(item_ref)
        if key is None:
            return None
        if key.list_id and key.list_id in self._items_by_list_id:
            return self._items_by_list_id[key.list_id]
        if key.full_name:
            cached = self._items_by_full_name.get(key.full_name.lower())
            if key.full_name.lower() in self._items_by_full_name:
                return cached
        return None

    def store_item(self, item: dict[str, Any]) -> None:
        """Index one item query result by ListID and FullName."""
        list_id = item.get("ListID")
        full_name = item.get("FullName") or item.get("Name")
        if list_id:
            self._items_by_list_id[list_id] = item
        if full_name:
            self._items_by_full_name[full_name.lower()] = item

    def mark_item_missing(self, key: ItemRefKey) -> None:
        """Record a cache miss for one ItemRef key."""
        if key.list_id:
            self._items_by_list_id[key.list_id] = None
        if key.full_name:
            self._items_by_full_name[key.full_name.lower()] = None

    def has_uom_set(self, list_id: str) -> bool:
        """Return whether a UnitOfMeasureSet ListID is already cached."""
        return list_id in self._uom_sets

    def lookup_uom_set(self, list_id: str) -> dict[str, Any] | None:
        """Return a cached UnitOfMeasureSet, or None when unknown or missing."""
        if list_id not in self._uom_sets:
            return None
        return self._uom_sets[list_id]

    def store_uom_set(self, uom_set: dict[str, Any]) -> None:
        """Index one UnitOfMeasureSet query result by ListID."""
        list_id = uom_set.get("ListID")
        if list_id:
            self._uom_sets[list_id] = uom_set

    def mark_uom_set_missing(self, list_id: str) -> None:
        """Record a cache miss for one UnitOfMeasureSet ListID."""
        self._uom_sets[list_id] = None


class UomBatchPreparer:
    """Batch-fetch missing UOM lookup data then rescale staged records."""

    def __init__(self, sink: Any, cache: UomLookupCache) -> None:
        self._sink = sink
        self._cache = cache
        self._request_counter = 0

    def _next_request_id(self, prefix: str) -> str:
        self._request_counter += 1
        return f"uom-{prefix}-{self._request_counter}"

    def prepare_and_rescale(
        self,
        records: list[dict[str, Any]],
        stream: str,
    ) -> list[dict[str, Any]]:
        """Prefetch UOM data for the batch and rescale each staged payload."""
        valid_records = [record for record in records if "preprocess_error" not in record]
        if not valid_records:
            return records

        item_refs = self._collect_item_refs(valid_records, stream)
        item_error = self._fetch_missing_items(item_refs)
        if item_error is not None:
            missing_refs = {
                ref
                for ref in item_refs
                if not self._cache.has_item_ref(
                    {"ListID": ref.list_id, "FullName": ref.full_name}
                )
            }
            for staged in valid_records:
                if self._record_uses_item_refs(staged, stream, missing_refs):
                    staged["preprocess_error"] = item_error

        still_valid = [record for record in valid_records if "preprocess_error" not in record]
        uom_set_ids = self._collect_uom_set_ids(still_valid, stream)
        uom_error = self._fetch_missing_uom_sets(uom_set_ids)
        if uom_error is not None:
            missing_sets = {
                list_id for list_id in uom_set_ids if not self._cache.has_uom_set(list_id)
            }
            for staged in still_valid:
                if self._record_uses_uom_sets(staged, stream, missing_sets):
                    staged["preprocess_error"] = uom_error

        for staged in valid_records:
            if "preprocess_error" in staged:
                continue
            error = rescale_payload_uom_quantities(
                staged["payload"],
                stream,
                lookup_item=self._cache.lookup_item,
                lookup_uom_set=self._cache.lookup_uom_set,
            )
            if error is not None:
                staged["preprocess_error"] = error
        return records

    def _collect_item_refs(
        self,
        records: list[dict[str, Any]],
        stream: str,
    ) -> list[ItemRefKey]:
        """Collect unique ItemRefs from lines that need UOM rescaling."""
        refs: dict[ItemRefKey, None] = {}
        for staged in records:
            for line in iter_uom_lines(staged["payload"], stream):
                key = item_ref_key(line.get("ItemRef") or {})
                if key is not None:
                    refs[key] = None
        return list(refs)

    def _collect_uom_set_ids(
        self,
        records: list[dict[str, Any]],
        stream: str,
    ) -> list[str]:
        """Collect UnitOfMeasureSet ListIDs referenced by batch lines."""
        uom_set_ids: dict[str, None] = {}
        for staged in records:
            for line in iter_uom_lines(staged["payload"], stream):
                item = self._cache.lookup_item(line.get("ItemRef") or {})
                if not item:
                    continue
                list_id = (item.get("UnitOfMeasureSetRef") or {}).get("ListID")
                if list_id:
                    uom_set_ids[list_id] = None
        return list(uom_set_ids)

    def _record_uses_item_refs(
        self,
        staged: dict[str, Any],
        stream: str,
        item_refs: set[ItemRefKey],
    ) -> bool:
        """Return whether a staged record references any of the given ItemRefs."""
        for line in iter_uom_lines(staged["payload"], stream):
            key = item_ref_key(line.get("ItemRef") or {})
            if key is not None and key in item_refs:
                return True
        return False

    def _record_uses_uom_sets(
        self,
        staged: dict[str, Any],
        stream: str,
        uom_set_ids: set[str],
    ) -> bool:
        """Return whether a staged record's items reference any of the given UOM sets."""
        for line in iter_uom_lines(staged["payload"], stream):
            item = self._cache.lookup_item(line.get("ItemRef") or {})
            if not item:
                continue
            list_id = (item.get("UnitOfMeasureSetRef") or {}).get("ListID")
            if list_id in uom_set_ids:
                return True
        return False

    def _fetch_missing_items(self, item_refs: list[ItemRefKey]) -> InvalidPayloadError | None:
        """Batch-query uncached items in one QBXML message."""
        missing = [ref for ref in item_refs if not self._cache.has_item_ref(
            {"ListID": ref.list_id, "FullName": ref.full_name}
        )]
        if not missing:
            return None

        query_elements: list[dict[str, Any]] = []
        ref_by_request_id: dict[str, ItemRefKey] = {}
        for ref in missing:
            request_id = self._next_request_id("item")
            ref_by_request_id[request_id] = ref
            if ref.list_id:
                query_body: dict[str, Any] = {"ListID": ref.list_id}
            else:
                query_body = {"FullName": ref.full_name}
            query_elements.append(
                {
                    "ItemQueryRq": {
                        "@requestID": request_id,
                        **query_body,
                    }
                }
            )

        self._sink.logger.info(
            "%s batch uom item lookup: %d item(s)",
            self._sink.name,
            len(query_elements),
        )
        try:
            qbxml_msgs_rs = self._sink.send_qbxml_batch(query_elements)
            responses = normalize_rs_list(qbxml_msgs_rs, "ItemQueryRs")
            responses_by_id = {
                response.get("@requestID", ""): response for response in responses
            }
        except Exception as exc:
            mapped = self._sink.map_qbwc_error(exc)
            self._sink.logger.warning(
                "UOM item lookup batch failed for %s (%d item(s)): %s",
                self._sink.name,
                len(missing),
                mapped,
            )
            return InvalidPayloadError(f"UOM item lookup failed: {mapped}")

        for request_id, ref in ref_by_request_id.items():
            matches = _interpret_query_rs(responses_by_id.get(request_id), "ItemRet")
            if len(matches) == 1:
                self._cache.store_item(matches[0])
            else:
                self._cache.mark_item_missing(ref)
        return None

    def _fetch_missing_uom_sets(self, uom_set_ids: list[str]) -> InvalidPayloadError | None:
        """Batch-query uncached UnitOfMeasureSets in one QBXML message."""
        missing = [list_id for list_id in uom_set_ids if not self._cache.has_uom_set(list_id)]
        if not missing:
            return None

        query_elements: list[dict[str, Any]] = []
        for list_id in missing:
            query_elements.append(
                {
                    "UnitOfMeasureSetQueryRq": {
                        "@requestID": self._next_request_id("uom-set"),
                        "ListID": list_id,
                    }
                }
            )

        self._sink.logger.info(
            "%s batch uom set lookup: %d set(s)",
            self._sink.name,
            len(query_elements),
        )
        try:
            qbxml_msgs_rs = self._sink.send_qbxml_batch(query_elements)
            responses = normalize_rs_list(qbxml_msgs_rs, "UnitOfMeasureSetQueryRs")
            responses_by_id = {
                response.get("@requestID", ""): response for response in responses
            }
        except Exception as exc:
            mapped = self._sink.map_qbwc_error(exc)
            self._sink.logger.warning(
                "UOM set lookup batch failed for %s (%d set(s)): %s",
                self._sink.name,
                len(missing),
                mapped,
            )
            return InvalidPayloadError(f"UOM set lookup failed: {mapped}")

        fetched_ids: set[str] = set()
        for response in responses_by_id.values():
            for uom_set in _interpret_query_rs(response, "UnitOfMeasureSetRet"):
                list_id = uom_set.get("ListID")
                if list_id:
                    fetched_ids.add(list_id)
                    self._cache.store_uom_set(uom_set)

        for list_id in missing:
            if list_id not in fetched_ids:
                self._cache.mark_uom_set_missing(list_id)
        return None
