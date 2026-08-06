"""QBWC target class."""

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from qbwc_common.config import DEFAULT_IS_SANDBOX, DEFAULT_REQUEST_TIMEOUT

from target_qbwc.client import DEFAULT_BATCH_SIZE
from target_qbwc.sinks import CustomersSink


class TargetQbwc(TargetHotglue):
    """Singer target for QuickBooks Desktop via QBWC."""

    SINK_TYPES = [CustomersSink]
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
    ).to_dict()


if __name__ == "__main__":
    TargetQbwc.cli()
