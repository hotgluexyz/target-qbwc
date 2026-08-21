"""Bill line preprocessing, mod reconciliation and customData mapping."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from qbwc_common import filter_dict_for_mod, get_mod_element_names

from target_qbwc.line_utils import normalize_line_list


@dataclass(frozen=True)
class _BillLineSide:
    """QBXML keys for one bill line array (item or expense)."""

    incoming_key: str
    ret_key: str
    mod_key: str
    clear_key: str
    mod_element: str
    custom_data_key: str


@dataclass(frozen=True)
class BillLineRef:
    """One payload line's externalId and request TxnLineID for response mapping."""

    external_id: str | None
    txn_line_id: str | None


@dataclass(frozen=True)
class BillLineMapping:
    """Line refs plus pre-existing TxnLineIDs used to map add and mod responses."""

    refs: tuple[BillLineRef, ...]
    existing_txn_line_ids: frozenset[str]


EMPTY_BILL_LINE_MAPPING = BillLineMapping(refs=(), existing_txn_line_ids=frozenset())

_ITEM_LINES = _BillLineSide(
    incoming_key="ItemLineAdd",
    ret_key="ItemLineRet",
    mod_key="ItemLineMod",
    clear_key="ClearItemLines",
    mod_element="ItemLineMod",
    custom_data_key="itemLines",
)
_EXPENSE_LINES = _BillLineSide(
    incoming_key="ExpenseLineAdd",
    ret_key="ExpenseLineRet",
    mod_key="ExpenseLineMod",
    clear_key="ClearExpenseLines",
    mod_element="ExpenseLineMod",
    custom_data_key="expenseLines",
)
_LINE_SIDES = (_ITEM_LINES, _EXPENSE_LINES)


def payload_has_bill_lines(payload: dict[str, Any]) -> bool:
    """Return whether the payload includes bill line arrays to reconcile."""
    return any(side.incoming_key in payload for side in _LINE_SIDES)


def _strip_add_line_metadata(lines: Any) -> tuple[list[dict[str, Any]], BillLineMapping]:
    """Strip TxnLineID and collect index-aligned externalIds from add lines."""
    normalized = normalize_line_list(lines)
    refs: list[BillLineRef] = []
    for line in normalized:
        line.pop("TxnLineID", None)
        external_id = line.pop("externalId", None)
        refs.append(BillLineRef(external_id=external_id, txn_line_id=None))
    return normalized, BillLineMapping(refs=tuple(refs), existing_txn_line_ids=frozenset())


def _apply_add_line_side(
    payload: dict[str, Any],
    side: _BillLineSide,
) -> BillLineMapping:
    """Normalize one add line array and return collected line mapping context."""
    lines, mapping = _strip_add_line_metadata(payload.get(side.incoming_key))
    if lines:
        payload[side.incoming_key] = lines
    else:
        payload.pop(side.incoming_key, None)
    return mapping


def preprocess_bill_add_payload(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], BillLineMapping, BillLineMapping]:
    """Strip add-line TxnLineIDs and collect line mapping context before encoding."""
    prepared = copy.deepcopy(payload)
    item_mapping = _apply_add_line_side(prepared, _ITEM_LINES)
    expense_mapping = _apply_add_line_side(prepared, _EXPENSE_LINES)
    return prepared, item_mapping, expense_mapping


def _reconcile_bill_line_side(
    merged: dict[str, Any],
    incoming: dict[str, Any],
    existing_lines: list[dict[str, Any]],
    side: _BillLineSide,
    schemas: Any,
) -> BillLineMapping:
    """Reconcile one bill line side for mod and return mapping context."""
    if side.incoming_key not in incoming:
        return EMPTY_BILL_LINE_MAPPING

    mod_allowed = get_mod_element_names(schemas, side.mod_element)
    payload_lines = [
        copy.deepcopy(line)
        for line in normalize_line_list(incoming[side.incoming_key])
    ]
    reconciled_lines: list[dict[str, Any]] = []
    refs: list[BillLineRef] = []
    existing_txn_line_ids = frozenset(
        line["TxnLineID"] for line in existing_lines if line.get("TxnLineID")
    )

    for existing_line in existing_lines:
        payload_item = next(
            (
                item
                for item in payload_lines
                if item.get("TxnLineID") == existing_line.get("TxnLineID")
            ),
            None,
        )
        if payload_item is None:
            continue

        updated_line = dict(existing_line)
        updated_line.update(payload_item)
        refs.append(
            BillLineRef(
                external_id=updated_line.pop("externalId", None),
                txn_line_id=updated_line.get("TxnLineID"),
            )
        )
        reconciled_lines.append(
            {key: value for key, value in updated_line.items() if key in mod_allowed}
        )
        payload_lines.remove(payload_item)

    for payload_line in payload_lines:
        payload_line["TxnLineID"] = "-1"
        refs.append(
            BillLineRef(
                external_id=payload_line.pop("externalId", None),
                txn_line_id="-1",
            )
        )
        reconciled_lines.append(
            {key: value for key, value in payload_line.items() if key in mod_allowed}
        )

    merged.pop(side.mod_key, None)
    merged.pop(side.clear_key, None)
    if reconciled_lines:
        merged[side.mod_key] = reconciled_lines
    else:
        merged[side.clear_key] = "true"

    return BillLineMapping(refs=tuple(refs), existing_txn_line_ids=existing_txn_line_ids)


def merge_bill_for_mod(
    existing: dict[str, Any],
    incoming: dict[str, Any],
    schemas: Any,
    id_field: str,
) -> tuple[dict[str, Any], BillLineMapping, BillLineMapping]:
    """Build a BillMod payload with line reconciliation when lines are present."""
    allowed = get_mod_element_names(schemas, "BillMod")
    merged = filter_dict_for_mod(existing, allowed)
    overlay = copy.deepcopy(incoming)
    for key in ("ListID", "TxnID", _ITEM_LINES.incoming_key, _EXPENSE_LINES.incoming_key):
        overlay.pop(key, None)
    merged.update(overlay)
    merged[id_field] = existing[id_field]
    merged["EditSequence"] = existing["EditSequence"]
    merged.pop("VendorAddress", None)

    item_mapping = _reconcile_bill_line_side(
        merged,
        incoming,
        normalize_line_list(existing.get(_ITEM_LINES.ret_key)),
        _ITEM_LINES,
        schemas,
    )
    expense_mapping = _reconcile_bill_line_side(
        merged,
        incoming,
        normalize_line_list(existing.get(_EXPENSE_LINES.ret_key)),
        _EXPENSE_LINES,
        schemas,
    )
    return merged, item_mapping, expense_mapping


def build_bill_line_custom_data(
    entity: dict[str, Any],
    item_mapping: BillLineMapping,
    expense_mapping: BillLineMapping,
) -> dict[str, Any]:
    """Build bookmark customData for bill add/mod success responses."""
    custom_data: dict[str, Any] = {}

    for mapping, side in (
        (item_mapping, _ITEM_LINES),
        (expense_mapping, _EXPENSE_LINES),
    ):
        line_items = _map_line_external_ids(mapping, entity.get(side.ret_key))
        if line_items:
            custom_data[side.custom_data_key] = line_items

    return custom_data


def _map_line_external_ids(
    mapping: BillLineMapping,
    qb_lines: Any,
) -> list[dict[str, str]]:
    """Map payload line externalIds to QB response TxnLineIDs."""
    if not mapping.refs:
        return []

    qb_list = normalize_line_list(qb_lines)
    qb_by_id = {
        line["TxnLineID"]: line for line in qb_list if line.get("TxnLineID")
    }
    used_ids: set[str] = set()
    line_items: list[dict[str, str]] = []
    pending_new: list[BillLineRef] = []

    for ref in mapping.refs:
        txn_line_id = ref.txn_line_id
        if txn_line_id and txn_line_id != "-1":
            qb_line = qb_by_id.get(txn_line_id)
            if qb_line and qb_line.get("TxnLineID"):
                used_ids.add(qb_line["TxnLineID"])
                if ref.external_id:
                    line_items.append(
                        {"externalId": ref.external_id, "id": qb_line["TxnLineID"]}
                    )
            continue
        pending_new.append(ref)

    new_qb_lines = [
        line
        for line in qb_list
        if line.get("TxnLineID")
        and line["TxnLineID"] not in mapping.existing_txn_line_ids
        and line["TxnLineID"] not in used_ids
    ]
    for ref, qb_line in zip(pending_new, new_qb_lines):
        if ref.external_id:
            line_items.append(
                {"externalId": ref.external_id, "id": qb_line["TxnLineID"]}
            )
    return line_items
