"""Shared helpers for QBXML line and unit list shapes."""

from __future__ import annotations

from typing import Any


def normalize_line_list(lines: Any) -> list[dict[str, Any]]:
    """Return line dicts as a list regardless of QBXML single-or-plural shape."""
    if lines is None:
        return []
    if isinstance(lines, dict):
        return [lines]
    return list(lines)


def normalize_unit_list(units: Any) -> list[dict[str, Any]]:
    """Return UOM unit dicts as a list regardless of QBXML single-or-plural shape."""
    if units is None:
        return []
    if isinstance(units, dict):
        return [units]
    return list(units)
