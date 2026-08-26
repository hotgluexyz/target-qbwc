"""Customer FullName lookup helpers for sub-customer/job payloads."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


def customer_full_name_for_lookup(
    payload: dict[str, Any],
    parent_full_name_resolver: Callable[[str], str | None] | None = None,
) -> str | None:
    """
    Build the QBD FullName used to look up a customer.

    Sub-customers/jobs are stored in QBD as `{ParentFullName}:{Name}` while
    upstream payloads typically send the short Name plus ParentRef.
    """
    name = payload.get("Name")
    if not name:
        return None

    parent_ref = payload.get("ParentRef") or {}
    parent_full_name = parent_ref.get("FullName")
    if parent_full_name:
        qualified_name = (
            name if name.startswith(f"{parent_full_name}:") else f"{parent_full_name}:{name}"
        )
        logger.info("Built customer lookup FullName from ParentRef.FullName: %s", qualified_name)
        return qualified_name

    parent_list_id = parent_ref.get("ListID")
    if parent_list_id and parent_full_name_resolver:
        resolved_parent_full_name = parent_full_name_resolver(parent_list_id)
        if resolved_parent_full_name:
            qualified_name = (
                name
                if name.startswith(f"{resolved_parent_full_name}:")
                else f"{resolved_parent_full_name}:{name}"
            )
            logger.info(
                "Built customer lookup FullName from ParentRef.ListID=%s: %s",
                parent_list_id,
                qualified_name,
            )
            return qualified_name

        logger.warning(
            "Customer parent not found for ParentRef.ListID=%s. Falling back to Name=%s for lookup.",
            parent_list_id,
            name,
        )

    return name
