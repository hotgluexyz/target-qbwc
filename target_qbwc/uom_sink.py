"""UOM quantity rescaling mixin for transaction upsert sinks."""

from __future__ import annotations

from typing import Any

from target_qbwc.uom_lookup import UomBatchPreparer
from target_qbwc.uom_quantities import UOM_LINE_KEYS


class QbwcUomTxnMixin:
    """Mixin that rescales transaction line quantities to QuickBooks base units."""

    def _apply_uom_rescaling(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rescale line quantities on staged records before lookup and write."""
        if self.name not in UOM_LINE_KEYS:
            return records

        cache = self._target.uom_lookup_cache
        preparer = UomBatchPreparer(self, cache)
        return preparer.prepare_and_rescale(records, self.name)

    def make_batch_request(self, records: list[dict]) -> dict:
        """Apply UOM rescaling before the upsert lookup and write batch."""
        return super().make_batch_request(self._apply_uom_rescaling(records))
