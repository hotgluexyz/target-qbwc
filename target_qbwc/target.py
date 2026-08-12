"""QBWC target class."""

from collections import Counter

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from qbwc_common import (
    DEFAULT_IS_SANDBOX,
    DEFAULT_REQUEST_TIMEOUT,
    QBWCClient,
    load_qbd_xml_schemas,
)

from target_qbwc.client import DEFAULT_BATCH_SIZE
from target_qbwc.uom_lookup import UomLookupCache
from target_qbwc.input import collect_input, run_ordered_streams
from target_qbwc.sinks import (
    BillsSink,
    CreditMemosSink,
    CustomersSink,
    InvoicesSink,
    ItemInventorySink,
    ItemNonInventorySink,
    ItemSalesTaxSink,
    JournalEntriesSink,
    SalesOrdersSink,
    SalesReceiptsSink,
    VendorCreditsSink,
    VendorsSink,
)


class TargetQbwc(TargetHotglue):
    """Singer target for QuickBooks Desktop via QBWC."""

    MAX_PARALLELISM = 1

    SINK_TYPES = [
        CustomersSink,
        VendorsSink,
        ItemInventorySink,
        ItemNonInventorySink,
        ItemSalesTaxSink,
        InvoicesSink,
        SalesOrdersSink,
        SalesReceiptsSink,
        CreditMemosSink,
        BillsSink,
        VendorCreditsSink,
        JournalEntriesSink,
    ]
    KNOWN_STREAMS = frozenset(sink_class.name for sink_class in SINK_TYPES)
    name = "target-qbwc"
    alerting_level = AlertingLevel.ERROR

    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.StringType,
            required=True,
            description="Base64-encoded connector token for the QBWC SOAP service",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=DEFAULT_REQUEST_TIMEOUT,
            description="Seconds to wait for a QBWC request to complete",
        ),
        th.Property(
            "is_sandbox",
            th.BooleanType,
            default=DEFAULT_IS_SANDBOX,
            description="Whether to use the QA sandbox environment",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            default=DEFAULT_BATCH_SIZE,
            description="Maximum records per QBXML batch message",
        ),
        th.Property(
            "input_path",
            th.StringType,
            description="Directory of entity JSON files (customer.json, invoice.json, ...)",
        ),
    ).to_dict()

    @property
    def uom_lookup_cache(self) -> UomLookupCache:
        """Return the shared UOM item and UnitOfMeasureSet lookup cache for this job."""
        cache = getattr(self, "_uom_lookup_cache", None)
        if cache is None:
            cache = UomLookupCache()
            self._uom_lookup_cache = cache
        return cache

    @property
    def qbwc_client(self) -> QBWCClient:
        """Return one authenticated QBWC client shared by all stream sinks."""
        client = getattr(self, "_qbwc_client", None)
        if client is None:
            client = QBWCClient(
                dict(self.config),
                load_qbd_xml_schemas(),
                self.logger,
            )
            client.create_session()
            self._qbwc_client = client
        return client

    def _process_lines(self, file_input):
        """Load merged input sources and feed each stream through the SDK reader in order."""
        records = collect_input(
            self.config,
            file_input,
            self.KNOWN_STREAMS,
        )
        run_ordered_streams(self, records)
        return Counter()


if __name__ == "__main__":
    TargetQbwc.cli()
