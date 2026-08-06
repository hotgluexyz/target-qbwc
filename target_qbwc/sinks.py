"""QBWC target sink classes."""

from target_qbwc.client_upsert import QbwcListUpsertBatchSink


class CustomersSink(QbwcListUpsertBatchSink):
    """Writes customer records to QuickBooks Desktop."""

    name = "customer"
    qbxml_entity = "Customer"
