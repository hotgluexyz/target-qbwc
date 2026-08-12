"""Unit tests for Singer and entity JSON input loading."""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, call

import jsonschema
import pytest

from target_qbwc.input import (
    STREAM_ORDER,
    _schema_from_records,
    collect_input,
    load_entity_json_dir,
    load_singer_stdin,
    merge_records_by_stream,
    run_ordered_streams,
    stream_name_from_filename,
)
from target_qbwc.target import TargetQbwc

KNOWN_STREAMS = TargetQbwc.KNOWN_STREAMS


def test_stream_order_matches_known_streams():
    """Every ordered stream has a registered sink."""
    assert set(STREAM_ORDER) <= KNOWN_STREAMS


def test_schema_from_records_supports_nested_invoice_fields():
    """Generated schemas accept nested QBXML refs and line items."""
    record = {
        "externalId": "inv-1",
        "CustomerRef": {"ListID": "1", "FullName": "Customer"},
        "RefNumber": "INV-1",
        "TxnDate": "2026-08-07",
        "InvoiceLineAdd": [
            {
                "ItemRef": {"ListID": "2", "FullName": "Item"},
                "Quantity": "1",
                "Rate": "12.00",
            }
        ],
    }
    schema = _schema_from_records("invoice", [record])["schema"]

    jsonschema.validate(record, schema)


def test_stream_name_from_filename_strips_suffix_and_extension():
    """Filename parsing maps entity JSON filenames to stream names."""
    assert stream_name_from_filename("customer.json") == "customer"
    assert stream_name_from_filename("invoice-20260729.json") == "invoice"
    assert stream_name_from_filename(Path("vendor.json")) == "vendor"


def test_load_entity_json_dir_ignores_unknown_files(tmp_path: Path):
    """Only registered sink filenames are loaded from the input directory."""
    (tmp_path / "customer.json").write_text(
        json.dumps([{"externalId": "json-1", "Name": "A"}])
    )
    (tmp_path / "job-details.json").write_text(json.dumps({"job": "meta"}))
    (tmp_path / "invoice-20260729.json").write_text(
        json.dumps([{"externalId": "json-2", "RefNumber": "INV-1"}])
    )

    loaded = load_entity_json_dir(tmp_path, known_streams=KNOWN_STREAMS)

    assert set(loaded) == {"customer", "invoice"}
    assert loaded["customer"][0]["externalId"] == "json-1"
    assert loaded["invoice"][0]["externalId"] == "json-2"


def test_load_entity_json_dir_rejects_non_array(tmp_path: Path):
    """Entity JSON files must contain arrays."""
    (tmp_path / "customer.json").write_text(json.dumps({"Name": "A"}))

    with pytest.raises(ValueError, match="must contain a JSON array"):
        load_entity_json_dir(tmp_path, KNOWN_STREAMS)


def test_load_singer_stdin_buffers_records_and_ignores_state():
    """Singer stdin loading keeps RECORD lines and drops STATE."""
    payload = "\n".join(
        [
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "customer",
                    "schema": {"type": "object", "properties": {}},
                }
            ),
            json.dumps(
                {
                    "type": "RECORD",
                    "stream": "customer",
                    "record": {"externalId": "singer-1", "Name": "A"},
                }
            ),
            json.dumps({"type": "STATE", "value": {"bookmarks": {}}}),
            json.dumps(
                {
                    "type": "RECORD",
                    "stream": "invoice",
                    "record": {"externalId": "singer-2", "RefNumber": "INV-1"},
                }
            ),
        ]
    )

    loaded = load_singer_stdin(io.StringIO(payload))

    assert loaded == {
        "customer": [{"externalId": "singer-1", "Name": "A"}],
        "invoice": [{"externalId": "singer-2", "RefNumber": "INV-1"}],
    }


def test_merge_records_by_stream_puts_json_before_singer():
    """Merged stream lists keep JSON records ahead of Singer records."""
    merged = merge_records_by_stream(
        {"customer": [{"externalId": "json-1"}]},
        {"customer": [{"externalId": "singer-1"}], "vendor": [{"externalId": "singer-2"}]},
    )

    assert merged["customer"] == [{"externalId": "json-1"}, {"externalId": "singer-1"}]
    assert merged["vendor"] == [{"externalId": "singer-2"}]


def test_merge_records_by_stream_returns_json_when_singer_empty():
    """Merge avoids copying when only entity JSON is present."""
    json_by_stream = {"customer": [{"externalId": "json-1"}]}

    merged = merge_records_by_stream(json_by_stream, {})

    assert merged is json_by_stream


def test_merge_records_by_stream_returns_singer_when_json_empty():
    """Merge avoids copying when only Singer stdin is present."""
    singer_by_stream = {"customer": [{"externalId": "singer-1"}]}

    merged = merge_records_by_stream({}, singer_by_stream)

    assert merged is singer_by_stream


def test_collect_input_loads_singer_stdin_only():
    """collect_input works with Singer stdin and no input_path."""
    singer = json.dumps(
        {
            "type": "RECORD",
            "stream": "customer",
            "record": {"externalId": "singer-1", "Name": "A"},
        }
    )

    records = collect_input({}, io.StringIO(singer + "\n"), KNOWN_STREAMS)

    assert records == {"customer": [{"externalId": "singer-1", "Name": "A"}]}


def test_collect_input_skips_interactive_stdin_when_input_path_set(tmp_path: Path):
    """JSON-only runs do not block on an interactive terminal."""
    (tmp_path / "customer.json").write_text(
        json.dumps([{"externalId": "json-1", "Name": "A"}])
    )
    stdin = MagicMock()
    stdin.isatty.return_value = True
    stdin.__iter__ = MagicMock(side_effect=AssertionError("stdin must not be read"))

    records = collect_input(
        {"input_path": str(tmp_path)},
        stdin,
        KNOWN_STREAMS,
    )

    assert records == {"customer": [{"externalId": "json-1", "Name": "A"}]}


def test_collect_input_merges_json_and_singer(tmp_path: Path):
    """collect_input orchestrates directory loading and stdin buffering."""
    (tmp_path / "customer.json").write_text(
        json.dumps([{"externalId": "json-1", "Name": "A"}])
    )
    singer = json.dumps(
        {
            "type": "RECORD",
            "stream": "customer",
            "record": {"externalId": "singer-1", "Name": "B"},
        }
    )

    records = collect_input(
        {"input_path": str(tmp_path)},
        io.StringIO(singer + "\n"),
        KNOWN_STREAMS,
    )

    assert records["customer"] == [
        {"externalId": "json-1", "Name": "A"},
        {"externalId": "singer-1", "Name": "B"},
    ]


def test_collect_input_raises_when_both_sources_empty(tmp_path: Path):
    """An empty job fails fast when neither input source has records."""
    with pytest.raises(ValueError, match="No input records found"):
        collect_input({"input_path": str(tmp_path)}, io.StringIO(""), KNOWN_STREAMS)


def test_run_ordered_streams_follows_stream_order_and_drains_between_streams():
    """Streams run in STREAM_ORDER and each sink is drained before the next."""
    records_by_stream = {
        "invoice": [{"externalId": "inv-1", "RefNumber": "INV-1"}],
        "customer": [{"externalId": "cust-1", "Name": "A"}],
        "vendor": [{"externalId": "vend-1", "Name": "B"}],
    }
    customer_sink = MagicMock()
    customer_sink.current_size = 1
    vendor_sink = MagicMock()
    vendor_sink.current_size = 0
    invoice_sink = MagicMock()
    invoice_sink.current_size = 2

    target = MagicMock()
    target._sinks_active = {
        "customer": customer_sink,
        "vendor": vendor_sink,
        "invoice": invoice_sink,
    }

    def drain_side_effect(sink):
        sink.current_size = 0

    target.drain_one.side_effect = drain_side_effect

    run_ordered_streams(target, records_by_stream)

    processed_streams = [
        call.args[0]["stream"]
        for call in target._process_schema_message.call_args_list
    ]
    assert processed_streams == [
        stream for stream in STREAM_ORDER if stream in records_by_stream
    ]
    target.drain_one.assert_has_calls([call(customer_sink), call(invoice_sink)])
