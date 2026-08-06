# target-qbwc

`target-qbwc` is a Singer target for QuickBooks Desktop, built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK).

It writes QuickBooks-shaped records to QuickBooks Desktop through the QuickBooks Web Connector and `qbwc-soap-service`.

## Installation

```bash
pip install target-qbwc
```

Or directly from the repo:

```bash
pip install git+https://github.com/hotgluexyz/target-qbwc.git
```

## Configuration

| Field | Required | Description |
|---|---|---|
| `token` | Yes | Base64-encoded connector token for the QBWC SOAP service |
| `request_timeout` | No | Seconds to wait for a QBWC request to complete. Defaults to `1200` |
| `is_sandbox` | No | Use the QA sandbox environment. Defaults to `false` |
| `batch_size` | No | Maximum records per QBXML batch message. Defaults to `75` |

Example `config.json`:

```json
{
  "token": "your_connector_token_here",
  "is_sandbox": true,
  "request_timeout": 1200,
  "batch_size": 75
}
```

## Authentication

The connector `token` is a base64-encoded JSON blob issued by the Hotglue platform. It encodes the environment, tenant, flow, and API credentials the QBWC SOAP service needs to route requests to the right QuickBooks Web Connector session.

Obtain it from the Hotglue UI for the flow that owns the QBWC write connector. The token must point at the **write** flow. If read and write are split across flows, the token must reference the write flow or carry `write_flow_id`, or QBWC will never pick up enqueued requests.

## Supported Streams

Input is QuickBooks-shaped JSON, the same payload shape the legacy `qbd-connector` accepted. Singer stream names match legacy upload filenames (singular snake_case, for example `customer`, `invoice`, `item_inventory`). Field names, types, and length limits come from the bundled qbXML XSD in `qbwc-common`, not from a unified accounting schema. `externalId` is optional Hotglue metadata, stripped before encoding and echoed in output state.

| Stream | Add | Mod | Lookup (priority) | Id field | Notes |
|---|---|---|---|---|---|
| `customer` | `CustomerAddRq` | `CustomerModRq` | `ListID`, then `Name` | `ListID` | Upsert is always on: lookup by `ListID` or `Name` (`FullName` query), then add or mod. |

As more streams land, this table is the single place for per-stream facts (request names, lookup keys, id field, quirks). No separate section per stream.

## How it works

Writes are asynchronous. The target enqueues one QBXML message per batch to the QBWC SOAP service, then blocks until QuickBooks Web Connector polls, processes the request, and returns a response. In the sandbox, polling happens on a schedule of roughly two minutes. A normal run can sit idle for that long before output appears. That is expected, not a hang.

## Batching and error behaviour

Each batch is a single QBXML message with `onError="continueOnError"`. Every record carries a `requestID` that QuickBooks echoes on the matching `*Rs` element. One bad record fails on its own while the rest of the batch still commits.

Per-record outcomes land in the output Singer `STATE` message under `bookmarks.customer` (one entry per record) and in `summary.customer` (`success`, `fail`, `existing`, `updated`). Failures include `error` and `hg_error_class` (`InvalidPayloadError` for validation and QuickBooks rejections, `InvalidCredentialsError` for token problems).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Request timed out | QuickBooks Desktop was closed, the company file was locked, or Web Connector was not running |
| Queue full | Too many requests are already pending on the SOAP service (limit is 10) |
| `[3100]` name already in use | A customer with that `Name` already exists in the company file |
| XSD validation error before send | A field is missing, the wrong type, or exceeds its length limit in the qbXML schema |

Re-running the sample payload against a sandbox that already has those customer names will produce `3100` errors. Append a unique suffix to each `Name` (for example a Unix timestamp) before re-running.

## Usage

Pipe tap output directly into the target:

```bash
tap-your-source --config tap_config.json | target-qbwc --config config.json
```

Or run against the committed sample file:

```bash
cat sample_payload/customers.singer | target-qbwc --config .secrets/config.json \
    > .secrets/output.singer 2> .secrets/output.singer.log
```

## Developer Resources

Set up a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv/bin/pip install -e . ruff pytest
```

Run lint:

```bash
.venv/bin/ruff check .
```

Verify the CLI:

```bash
.venv/bin/target-qbwc --version
.venv/bin/target-qbwc --about
```

Run tests:

```bash
.venv/bin/pytest target_qbwc/tests/
```

Related repos:

- [`qbwc-common`](https://github.com/hotgluexyz/qbwc-common): shared QBWC client, XSD bundle, and QBXML helpers used by this target and `tap-qbwc`
- [`tap-qbwc`](https://github.com/hotgluexyz/tap-qbwc): Singer tap for reading from QuickBooks Desktop via QBWC
