"""Load Singer and entity JSON input, merge by stream, and run in dependency order."""

from __future__ import annotations

import json
import logging
from io import IOBase
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from hotglue_singer_sdk.io_base import SingerMessageType

if TYPE_CHECKING:
    from target_qbwc.target import TargetQbwc

logger = logging.getLogger(__name__)

STREAM_ORDER: tuple[str, ...] = (
    "customer",
    "vendor",
    "item_inventory",
    "item_noninventory",
    "item_sales_tax",
    "purchase_order",
    "sales_order",
    "invoice",
    "credit_memo",
    "bill",
    "sales_receipt",
    "vendor_credit",
    "journal_entry",
)

_OPEN_PROPERTY_SCHEMA: dict[str, Any] = {"type": ["string", "null"]}


def _property_schema(value: Any) -> dict[str, Any]:
    """Infer a permissive JSON Schema property from one record value."""
    if isinstance(value, dict):
        return {
            "type": ["object", "null"],
            "properties": {
                key: _property_schema(item) for key, item in value.items()
            },
        }
    if isinstance(value, list):
        item_schema = _property_schema(value[0]) if value else _OPEN_PROPERTY_SCHEMA
        return {"type": ["array", "null"], "items": item_schema}
    if isinstance(value, bool):
        return {"type": ["boolean", "null"]}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": ["integer", "null"]}
    if isinstance(value, float):
        return {"type": ["number", "null"]}
    return _OPEN_PROPERTY_SCHEMA


def stream_name_from_filename(path: str | Path) -> str:
    """Return the stream name encoded in an entity JSON filename."""
    return Path(path).stem.split("-")[0].lower()


def load_entity_json_dir(
    input_path: str | Path,
    known_streams: frozenset[str] | set[str],
) -> dict[str, list[dict[str, Any]]]:
    """Load QuickBooks-shaped record arrays from a directory of entity JSON files."""
    directory = Path(input_path)
    if not directory.is_dir():
        raise ValueError(f"input_path is not a directory: {input_path}")

    records_by_stream: dict[str, list[dict[str, Any]]] = {}
    for json_path in sorted(directory.glob("*.json")):
        stream_name = stream_name_from_filename(json_path)
        if stream_name not in known_streams:
            continue
        payload = json.loads(json_path.read_text())
        if not isinstance(payload, list):
            raise ValueError(
                f"Entity JSON file must contain a JSON array: {json_path.name}"
            )
        records_by_stream.setdefault(stream_name, []).extend(payload)
    return records_by_stream


def load_singer_stdin(file_input: TextIO | IOBase | None) -> dict[str, list[dict[str, Any]]]:
    """Buffer Singer RECORD messages from stdin and ignore input STATE."""
    records_by_stream: dict[str, list[dict[str, Any]]] = {}
    if file_input is None:
        return records_by_stream

    for line in file_input:
        line = line.strip()
        if not line:
            continue
        message = json.loads(line)
        if message.get("type") != SingerMessageType.RECORD:
            continue
        stream_name = message["stream"]
        records_by_stream.setdefault(stream_name, []).append(message["record"])
    return records_by_stream


def merge_records_by_stream(
    json_by_stream: dict[str, list[dict[str, Any]]],
    singer_by_stream: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Merge JSON and Singer records per stream with JSON records first."""
    if not json_by_stream:
        return singer_by_stream
    if not singer_by_stream:
        return json_by_stream

    merged: dict[str, list[dict[str, Any]]] = {}
    for stream_name in set(json_by_stream) | set(singer_by_stream):
        json_records = json_by_stream.get(stream_name, [])
        singer_records = singer_by_stream.get(stream_name, [])
        if not singer_records:
            merged[stream_name] = json_records
        elif not json_records:
            merged[stream_name] = singer_records
        else:
            merged[stream_name] = json_records + singer_records
    return merged


def _skip_stdin_for_json_only(
    input_path: str | Path | None,
    stdin: TextIO | IOBase | None,
) -> bool:
    """Skip blocking on an interactive terminal when entity JSON is the sole input."""
    if not input_path or stdin is None:
        return False
    isatty = getattr(stdin, "isatty", None)
    return bool(isatty and isatty())


def collect_input(
    config: dict[str, Any],
    stdin: TextIO | IOBase | None,
    known_streams: frozenset[str] | set[str],
) -> dict[str, list[dict[str, Any]]]:
    """Load entity JSON and Singer stdin, merge them, and return {} when both are empty."""
    json_by_stream: dict[str, list[dict[str, Any]]] = {}
    input_path = config.get("input_path")
    if input_path:
        json_by_stream = load_entity_json_dir(input_path, known_streams)

    singer_stdin = None if _skip_stdin_for_json_only(input_path, stdin) else stdin
    singer_by_stream = load_singer_stdin(singer_stdin)
    merged = merge_records_by_stream(json_by_stream, singer_by_stream)
    if not merged or not any(records for records in merged.values()):
        logger.info("No input records found in stdin or input_path; nothing to export.")
        return {}
    return merged


def _schema_from_records(stream_name: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a minimal open Singer SCHEMA message inferred from record shapes."""
    properties: dict[str, Any] = {}
    for record in records:
        for key, value in record.items():
            if key not in properties:
                properties[key] = _property_schema(value)
    properties.setdefault("externalId", _OPEN_PROPERTY_SCHEMA)
    return {
        "type": SingerMessageType.SCHEMA,
        "stream": stream_name,
        "schema": {"type": ["object", "null"], "properties": properties},
        "key_properties": ["externalId"],
    }


def _streams_to_process(records_by_stream: dict[str, list[dict[str, Any]]]) -> list[str]:
    """Return stream names in processing order, with unknown streams after STREAM_ORDER."""
    ordered = [
        stream_name
        for stream_name in STREAM_ORDER
        if records_by_stream.get(stream_name)
    ]
    extra = sorted(
        stream_name
        for stream_name in records_by_stream
        if stream_name not in STREAM_ORDER and records_by_stream.get(stream_name)
    )
    return ordered + extra


def _drain_stream(target: TargetQbwc, stream_name: str) -> None:
    """Flush any buffered records for one stream before moving to the next."""
    sink = target._sinks_active.get(stream_name)
    if not sink:
        return
    while sink.current_size > 0:
        target.drain_one(sink)


def _process_stream_records(
    target: TargetQbwc,
    stream_name: str,
    records: list[dict[str, Any]],
) -> None:
    """Feed one stream's schema and records through the SDK message handlers."""
    target._process_schema_message(_schema_from_records(stream_name, records))
    for record in records:
        target._process_record_message(
            {
                "type": SingerMessageType.RECORD,
                "stream": stream_name,
                "record": record,
            }
        )


def run_ordered_streams(
    target: TargetQbwc,
    records_by_stream: dict[str, list[dict[str, Any]]],
) -> None:
    """Process each stream in STREAM_ORDER, draining completely before the next."""
    for stream_name in _streams_to_process(records_by_stream):
        records = records_by_stream[stream_name]
        target.logger.info("Processing stream '%s' (%s records)", stream_name, len(records))
        _process_stream_records(target, stream_name, records)
        _drain_stream(target, stream_name)
