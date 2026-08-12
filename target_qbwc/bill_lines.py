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


def _strip_add_line_metadata(lines: Any) -> tuple[list[dict[str, Any]], list[str | None]]:
    """Strip TxnLineID and collect truthy externalIds from add lines."""
    normalized = normalize_line_list(lines)
    external_ids: list[str | None] = []
    for line in normalized:
        line.pop("TxnLineID", None)
        external_id = line.pop("externalId", None)
        if external_id:
            external_ids.append(external_id)
    return normalized, external_ids


def _apply_add_line_side(
    payload: dict[str, Any],
    side: _BillLineSide,
) -> list[str | None]:
    """Normalize one add line array and return collected externalIds."""
    lines, external_ids = _strip_add_line_metadata(payload.get(side.incoming_key))
    if lines:
        payload[side.incoming_key] = lines
    else:
        payload.pop(side.incoming_key, None)
    return external_ids


def preprocess_bill_add_payload(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[str | None], list[str | None]]:
    """Strip add-line TxnLineIDs and collect line externalIds before encoding."""
    prepared = copy.deepcopy(payload)
    item_external_ids = _apply_add_line_side(prepared, _ITEM_LINES)
    expense_external_ids = _apply_add_line_side(prepared, _EXPENSE_LINES)
    return prepared, item_external_ids, expense_external_ids


def _reconcile_bill_line_side(
    merged: dict[str, Any],
    incoming: dict[str, Any],
    existing_lines: list[dict[str, Any]],
    side: _BillLineSide,
    schemas: Any,
) -> list[str | None]:
    """Reconcile one bill line side for mod and return index-aligned externalIds."""
    if side.incoming_key not in incoming:
        return []

    mod_allowed = get_mod_element_names(schemas, side.mod_element)
    payload_lines = [
        copy.deepcopy(line)
        for line in normalize_line_list(incoming[side.incoming_key])
    ]
    reconciled_lines: list[dict[str, Any]] = []
    external_ids: list[str | None] = []

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
        external_ids.append(updated_line.pop("externalId", None))
        reconciled_lines.append(
            {key: value for key, value in updated_line.items() if key in mod_allowed}
        )
        payload_lines.remove(payload_item)

    for payload_line in payload_lines:
        payload_line["TxnLineID"] = "-1"
        external_ids.append(payload_line.pop("externalId", None))
        reconciled_lines.append(
            {key: value for key, value in payload_line.items() if key in mod_allowed}
        )

    merged.pop(side.mod_key, None)
    merged.pop(side.clear_key, None)
    if reconciled_lines:
        merged[side.mod_key] = reconciled_lines
    else:
        merged[side.clear_key] = "true"

    return external_ids


def merge_bill_for_mod(
    existing: dict[str, Any],
    incoming: dict[str, Any],
    schemas: Any,
    id_field: str,
) -> tuple[dict[str, Any], list[str | None], list[str | None]]:
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

    item_external_ids = _reconcile_bill_line_side(
        merged,
        incoming,
        normalize_line_list(existing.get(_ITEM_LINES.ret_key)),
        _ITEM_LINES,
        schemas,
    )
    expense_external_ids = _reconcile_bill_line_side(
        merged,
        incoming,
        normalize_line_list(existing.get(_EXPENSE_LINES.ret_key)),
        _EXPENSE_LINES,
        schemas,
    )
    return merged, item_external_ids, expense_external_ids


def build_bill_line_custom_data(
    entity: dict[str, Any],
    item_external_ids: list[str | None],
    expense_external_ids: list[str | None],
) -> dict[str, Any]:
    """Build bookmark customData for bill add/mod success responses."""
    custom_data: dict[str, Any] = {}

    for external_ids, side in (
        (item_external_ids, _ITEM_LINES),
        (expense_external_ids, _EXPENSE_LINES),
    ):
        line_items = _map_line_external_ids(external_ids, entity.get(side.ret_key))
        if line_items:
            custom_data[side.custom_data_key] = line_items

    return custom_data


def _map_line_external_ids(
    external_ids: list[str | None],
    qb_lines: Any,
) -> list[dict[str, str]]:
    """Map index-aligned externalIds to QB response line TxnLineIDs."""
    if not external_ids:
        return []

    line_items: list[dict[str, str]] = []
    for index, qb_line in enumerate(normalize_line_list(qb_lines)):
        external_id = external_ids[index] if index < len(external_ids) else None
        if external_id and qb_line.get("TxnLineID"):
            line_items.append(
                {
                    "externalId": external_id,
                    "id": qb_line["TxnLineID"],
                }
            )
    return line_items

