# target-qbwc

`target-qbwc` is a Singer target for QuickBooks Desktop, built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK).

It writes QuickBooks-shaped records to QuickBooks Desktop through the QuickBooks Web Connector and `qbwc-soap-service`. Payloads use QuickBooks qbXML field names (PascalCase), not a unified accounting schema. Stream names and file layout follow the same conventions as the legacy on-prem QuickBooks Desktop write connector.

## Prerequisites

Writing through QBWC is asynchronous. The target enqueues QBXML to the SOAP service, then waits until QuickBooks Web Connector polls, processes the request, and returns a response.

You need all of the following available for a write job to complete:

- QuickBooks Desktop open with the target company file
- QuickBooks Web Connector running and configured for your QBWC application
- A valid `token` for the `qbwc-soap-service` deployment you are targeting

For reads from the same company file, see [`tap-qbwc`](https://github.com/hotgluexyz/tap-qbwc). Both connectors share transport and QBXML helpers through [`qbwc-common`](https://github.com/hotgluexyz/qbwc-common).

## Installation

Clone the repo, create a virtual environment, and install in editable mode:

```bash
git clone https://github.com/hotgluexyz/target-qbwc.git
cd target-qbwc
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

This target depends on [`qbwc-common`](https://github.com/hotgluexyz/qbwc-common).

## Configuration

| Field | Required | Description |
|---|---|---|
| `token` | Yes | Base64-encoded connector token for the QBWC SOAP service |
| `request_timeout` | No | Seconds to wait for a QBWC request to complete. Defaults to `1200` |
| `is_sandbox` | No | Use the QA sandbox environment. Defaults to `false` |
| `batch_size` | No | Maximum records per QBXML batch message. Defaults to `100` |
| `input_path` | No | Directory of entity JSON files (`customer.json`, `invoice.json`, ...). See [Input formats and processing order](#input-formats-and-processing-order) |

Example `config.json`:

```json
{
  "token": "your_connector_token_here",
  "is_sandbox": true,
  "request_timeout": 1200,
  "batch_size": 100,
  "input_path": "sample_payload/entity_json"
}
```

## Authentication

The `token` is a base64-encoded JSON blob. It carries the environment, tenant, and API credentials the QBWC SOAP service needs to route requests to the correct QuickBooks Web Connector session. Pass it in config.

## Usage

Pipe tap output on stdin:

```bash
tap-your-source --config tap_config.json | target-qbwc --config config.json
```

Run against committed Singer fixtures:

```bash
cat sample_payload/customers.singer | target-qbwc --config config.json
cat sample_payload/multi_stream_out_of_order.singer | target-qbwc --config config.json
```

Run from entity JSON only (set `input_path` in config; no stdin required at an interactive terminal):

```bash
target-qbwc --config config.json
```

In scripts or CI where stdin is not a TTY, pipe an empty stream when using `input_path` alone:

```bash
target-qbwc --config config.json < /dev/null
```

## Input formats and processing order

The target accepts Singer stdin, entity JSON on disk, or both at once. There is no `input_format` setting; detection is implicit.

1. **Singer** — pipe a `.singer` file on stdin. Each `RECORD.stream` must match a sink `name` (singular snake_case, for example `customer`, `invoice`).
2. **Entity JSON** — set `input_path` to a directory of `customer.json`, `invoice.json`, and similar files. Each file is a JSON array of QuickBooks-shaped records. A dated suffix is allowed (`invoice-20260729.json` maps to stream `invoice`).

   ```json
   [
     {
       "externalId": "entity-cust-001",
       "Name": "Acme Corp",
       "CompanyName": "Acme Corporation",
       "BillAddress": {
         "Addr1": "100 Main St",
         "City": "Austin",
         "State": "TX",
         "PostalCode": "78701",
         "Country": "USA"
       }
     }
   ]
   ```

3. **Both** — when stdin has Singer lines and `input_path` has entity JSON, records are merged per stream. **JSON first, then Singer.** This matters when the same stream appears in both sources.
4. **Stream order** — streams always run in this order, one stream at a time (each sink is drained before the next starts):

   `customer` → `vendor` → `item_inventory` → `item_noninventory` → `item_sales_tax` → `sales_order` → `invoice` → `credit_memo` → `bill` → `sales_receipt` → `vendor_credit` → `journal_entry`

   Input file or line order does not matter.

## Payload contract

- Records use QuickBooks qbXML field names in PascalCase (`Name`, `CustomerRef`, `InvoiceLineAdd`, ...).
- Field names, types, and length limits come from the bundled qbXML XSD in [`qbwc-common`](https://github.com/hotgluexyz/qbwc-common).
- Optional `externalId` on a record is tracking metadata. It is stripped before encoding and echoed in output state on success.

Singer stream names are singular snake_case (for example `customer`, `invoice`, `item_inventory`).

## Upsert behaviour

Send Add-shaped payloads. Before each write, the target queries QuickBooks using the stream's lookup keys (see the table below). When a match is found, it issues a Mod with `ListID` or `TxnID` and `EditSequence` from the queried record. When no match is found, it issues an Add. You do not need to supply `EditSequence` or internal ids on update.

## Supported Streams

| Stream | Add | Mod | Lookup (priority) | Id field |
|---|---|---|---|---|
| `customer` | `CustomerAddRq` | `CustomerModRq` | `ListID`, then `Name` | `ListID` |
| `vendor` | `VendorAddRq` | `VendorModRq` | `ListID`, then `Name` | `ListID` |
| `item_inventory` | `ItemInventoryAddRq` | `ItemInventoryModRq` | `ListID`, then `Name` | `ListID` |
| `item_noninventory` | `ItemNonInventoryAddRq` | `ItemNonInventoryModRq` | `ListID`, then `Name` | `ListID` |
| `item_sales_tax` | `ItemSalesTaxAddRq` | `ItemSalesTaxModRq` | `ListID`, then `Name` | `ListID` |
| `sales_order` | `SalesOrderAddRq` | `SalesOrderModRq` | `TxnID`, then `RefNumber` | `TxnID` |
| `invoice` | `InvoiceAddRq` | `InvoiceModRq` | `TxnID`, then `RefNumber` | `TxnID` |
| `sales_receipt` | `SalesReceiptAddRq` | `SalesReceiptModRq` | `TxnID`, then `RefNumber` | `TxnID` |
| `credit_memo` | `CreditMemoAddRq` | `CreditMemoModRq` | `TxnID`, then `RefNumber` | `TxnID` |
| `bill` | `BillAddRq` | `BillModRq` | `TxnID`, then `RefNumber` (scoped by `VendorRef`) | `TxnID` |
| `vendor_credit` | `VendorCreditAddRq` | `VendorCreditModRq` | `TxnID`, then `RefNumber` | `TxnID` |
| `journal_entry` | `JournalEntryAddRq` | `JournalEntryModRq` | `TxnID`, then `RefNumber` | `TxnID` |

### Unit of measure quantity rescaling

For `invoice`, `bill`, and `credit_memo`, the target automatically rescales line `Quantity` to QuickBooks **base units** before writing when the line has `Quantity` and `ItemRef` and the item is linked to a `UnitOfMeasureSet`.

- Optional `UnitOfMeasure` on the line selects which unit the payload quantity is expressed in.
- When `UnitOfMeasure` is omitted, the target uses the item's **Sales** default unit on `invoice` and `credit_memo`, and the **Purchase** default on `bill`.
- If the line unit matches the UOM set base unit, the quantity is left unchanged.
- If the unit name is not valid for the item's UOM set, the record fails with `InvalidPayloadError` listing the valid units.

Before each write batch, the target batch-queries any uncached items and UOM sets (deduped within the batch). Results are cached for the rest of the job, so the same item or UOM set is not queried again on later batches or streams.

### Bill line `externalId` mapping

When a bill add or mod succeeds, set `externalId` on the bill and on each `ItemLineAdd` / `ExpenseLineAdd` line you want to track. Only lines that include `externalId` appear in the output mapping.

The bookmark then includes `customData` with `itemLines` and/or `expenseLines`, each an array of `{ "externalId": "<yours>", "id": "<TxnLineID>" }`. Use the line `id` values on later bill mods when you need to update specific lines by `TxnLineID`.

## Output and state

Per-record outcomes are written to the final Singer `STATE` message under `bookmarks.<stream>` (one entry per input record) and rolled up in `summary.<stream>` (`success`, `fail`, `existing`, `updated`).

Example excerpt after a mixed batch:

```json
{
  "type": "STATE",
  "value": {
    "bookmarks": {
      "customer": [
        {
          "success": true,
          "id": "80000001-1234567890",
          "externalId": "cust-001",
          "hash": "a1b2c3..."
        },
        {
          "success": false,
          "externalId": "cust-invalid",
          "error": "[3170] The string \"THIS-NAME-IS-WAY-TOO-LONG...\" is too long.",
          "hg_error_class": "InvalidPayloadError"
        }
      ]
    },
    "summary": {
      "customer": {
        "success": 1,
        "fail": 1,
        "existing": 0,
        "updated": 0
      }
    }
  }
}
```

Successful updates set `is_updated` on the bookmark. Failures include `error` and `hg_error_class` (`InvalidPayloadError` for validation and QuickBooks rejections, `InvalidCredentialsError` for token problems).

## Batching and errors

Each batch is a single QBXML message with `onError="continueOnError"`. Every record carries a `requestID` that QuickBooks echoes on the matching `*Rs` element. One bad record fails on its own while the rest of the batch still commits.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Request timed out or taking too long | QuickBooks Desktop was closed, the company file was locked, or Web Connector was not running |
| Queue full | Too many requests are already pending on the SOAP service (limit is 10) |
| XSD validation error before send | A field is missing, the wrong type, or exceeds its length limit in the qbXML schema |
| `[3100]` or similar QuickBooks status | Often a duplicate name or reference already present in the company file |

When re-running sample payloads against a company file that already contains those records, use unique `Name` or `RefNumber` values (for example append a timestamp) to avoid duplicate errors.

## Developer resources

Lint and test with `tox` (runs ruff and pytest):

```bash
tox
```

Or run tools directly from the virtual environment:

```bash
.venv/bin/ruff check .
.venv/bin/pytest target_qbwc/tests/
.venv/bin/target-qbwc --version
.venv/bin/target-qbwc --about
```

Committed fixtures under `sample_payload/`:

**Singer (stdin)**

| Path | Purpose |
|---|---|
| `customers.singer` | Customer add; includes one deliberate XSD `Name` length failure |
| `vendors.singer` | Vendor add (two records) |
| `item_inventory.singer` | Inventory item add (two records) |
| `invoices.singer` | Invoice add with line items (two records) |
| `bills.singer` | Bill add with item lines and line `externalId` |
| `journal_entries.singer` | Journal entry add with debit/credit lines |
| `multi_stream_out_of_order.singer` | Records in non-dependency order; target reorders by `STREAM_ORDER`. Uses `PLACEHOLDER_*` refs; edit before a live run |

**Entity JSON (`input_path`)**

| Path | Purpose |
|---|---|
| `entity_json/customer.json` | Customer add |
| `entity_json/vendor.json` | Vendor add (two records) |
| `entity_json/item_inventory.json` | Inventory item add (two records) |
| `entity_json/invoice.json` | Invoice add with line items (two records) |
| `entity_json/bill.json` | Bill add with item lines and line `externalId` |
| `entity_json/journal_entry.json` | Journal entry add with debit/credit lines |

Fixture notes:

- Run list entities (`customer`, `vendor`, `item_inventory`) before txn samples (`invoice`, `bill`). Invoice and bill entity JSON reference `Name` / `FullName` values from the other fixtures in this folder (or matching records already in your company file).
- Account refs in `entity_json/item_inventory.json` and `entity_json/journal_entry.json` match the QA sandbox company file. Adjust `IncomeAccountRef`, `COGSAccountRef`, `AssetAccountRef`, and journal `AccountRef` values for other companies.
- Re-runs upsert by lookup keys (`Name`, `RefNumber`, ...). Payloads are Add-shaped. Sending `InvoiceLineAdd` / `ItemLineAdd` again on an existing txn can fail XSD validation on mod; use new `RefNumber` values or mod-shaped line fields for updates.

Related repos:

- [`qbwc-common`](https://github.com/hotgluexyz/qbwc-common): shared QBWC client, XSD bundle, and QBXML helpers used by this target and `tap-qbwc`
- [`tap-qbwc`](https://github.com/hotgluexyz/tap-qbwc): Singer tap for reading from QuickBooks Desktop via QBWC
