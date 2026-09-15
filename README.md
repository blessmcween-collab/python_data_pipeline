# Data Processing Pipeline

A configuration-driven ETL pipeline in Python. It reads raw data from CSV files, JSON files or an HTTP API, validates and cleans it against a declared schema, applies transforms, and writes structured output alongside a machine-readable run report.

The point of the project is handling data that is *not* tidy. Real exports arrive with byte-order marks from Excel, `N/A` in numeric columns, prices written as `£1,200.50`, dates in three different formats in the same column, duplicate rows and values that are simply wrong. This pipeline repairs what it can, quarantines what it cannot, and tells you exactly what it did.

```
CSV / JSON / API  ->  clean & validate  ->  transform  ->  CSV / JSON / JSONL
                            |                                      |
                      rejected_rows.csv                     run_report.json
```

---

## Table of contents

- [Quick start](#quick-start)

- [What the sample run does](#what-the-sample-run-does)
- [Command line usage](#command-line-usage)
- [Configuration reference](#configuration-reference)
- [Reading from an API](#reading-from-an-api)
- [Project structure](#project-structure)
- [Design notes](#design-notes)
- [Testing](#testing)

---

## Quick start

Requires Python 3.10 or newer.

```bash
git clone https://github.com/YOUR-USERNAME/data-pipeline.git
cd data-pipeline

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements-dev.txt

python -m pipeline --config config.yaml
```

That processes the sample data in `data/input/` and writes results to `data/output/`. Expected output:

```
customer-orders: completed - 6 dataset(s), 48 row(s), 5 file(s) written in 0.07s
  warning: source 'customers': 6 row(s) quarantined
  warning: source 'orders': 7 row(s) quarantined
```

If you have `make`:

```bash
make install    # install dependencies
make run        # run the pipeline
make dry-run    # run every stage but write nothing
make test       # run the test suite
```

---

## What the sample run does

`data/input/customers.csv` and `data/input/orders.json` are deliberately messy. Every problem below is in the sample data and handled by the pipeline.

| Problem in the raw data | How the pipeline handles it |
|---|---|
| UTF-8 byte-order mark from Excel | `utf-8-sig` encoding, with fallbacks to cp1252 and latin-1 |
| Headers like `Customer ID`, `Full Name ` | normalised to `customer_id`, `full_name` |
| `N/A`, `-`, `unknown`, empty, whitespace-only | all become proper nulls |
| `£1,200.50`, `$850.00`, `2,500` | currency symbols and separators stripped, parsed as float |
| `(500)` | read as `-500.0` (accounting notation for a negative) |
| `2023-01-15`, `14/02/2023`, `03 Apr 2023`, `2023/05/20` | all parsed to ISO dates, day-first |
| `Yes`, `TRUE`, `1`, `no`, `N`, `0` | parsed to real booleans |
| `alice  johnson`, `  BOB SMITH ` | trimmed, collapsed, title-cased |
| Missing credit limit or age | filled with the column median |
| Age of `250`, quantity of `-3` | fail the declared range, row quarantined |
| Country `XX`, status `unknown_status` | fail the allowed-value list, row quarantined |
| Missing customer ID, blank name | required field missing, row quarantined |
| The same customer and order appearing twice | deduplicated on the declared key |
| Nested JSON (`shipping.country`) | flattened to `shipping_country` |
| Quantity written as `"two"` | cannot be coerced, row quarantined |

### Output files

| File | Contents |
|---|---|
| `customers_clean.csv` | validated customer records |
| `orders_clean.csv` | validated orders with a derived `line_total` |
| `customer_summary.csv` | spend, order count and last order date per customer |
| `monthly_revenue.json` | revenue and order count per month |
| `rejected_rows.csv` | every quarantined row, with the reason it failed |
| `run_report.json` | full statistics for the run |

Nothing is silently discarded. Every rejected row appears in the quarantine file with an explanation:

```csv
_source,_reject_reason,customer_id,full_name,...
customers,country: value outside allowed range/set,C009,Ivan Petrov,...
customers,age: value outside allowed range/set,C012,Lena Novak,...
orders,quantity: not a valid integer,C004,,...
orders,status: value outside allowed range/set,C999,,...
```

And `run_report.json` records what happened column by column, which is what you want when the pipeline runs unattended on a schedule:

```json
{
  "name": "customer-orders",
  "status": "completed",
  "duration_seconds": 0.067,
  "sources": {
    "customers": {
      "rows_in": 18,
      "rows_out": 11,
      "rows_rejected": 6,
      "duplicates_removed": 1,
      "fields": {
        "country": { "not_allowed": 1 },
        "credit_limit": { "filled": 3, "out_of_range": 1 },
        "age": { "coercion_failures": 1, "filled": 2, "out_of_range": 1 }
      }
    }
  }
}
```

---

## Command line usage

```
python -m pipeline [-h] [-c CONFIG] [-o OUTPUT_DIR] [-l LEVEL] [--dry-run] [--fail-fast] [--quiet]
```

| Flag | Effect |
|---|---|
| `-c`, `--config` | path to the YAML config (default `config.yaml`) |
| `-o`, `--output-dir` | override the output directory |
| `-l`, `--log-level` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `--dry-run` | run every stage, write nothing to disk |
| `--fail-fast` | stop at the first error instead of carrying on |
| `--quiet` | print only the final summary |

Exit codes, so the pipeline can be used in a cron job or CI step:

| Code | Meaning |
|---|---|
| `0` | completed cleanly |
| `1` | completed, but with recoverable errors |
| `2` | the run failed |
| `3` | the configuration is invalid |

### Environment overrides

Any config value can be overridden without editing the file. Use the `PIPELINE__` prefix and `__` as a path separator:

```bash
PIPELINE__LOGGING__LEVEL=DEBUG python -m pipeline -c config.yaml
PIPELINE__PIPELINE__OUTPUT_DIR=/tmp/out python -m pipeline -c config.yaml
```

This keeps one config file working across local, staging and production.

### Logging

Two destinations. The console shows the level you asked for; the rotating file at `logs/pipeline.log` always records `DEBUG`, up to 1 MB per file with three backups.

```
20:52:11 | INFO     | pipeline.readers  | Source 'customers': read 18 row(s) from customers.csv
20:52:11 | WARNING  | pipeline.cleaning | Source 'customers': 3 value(s) in 'credit_limit' could not be read as float
20:52:11 | INFO     | pipeline.cleaning | Source 'customers': filled 3 missing value(s) in 'credit_limit' using 'median'
20:52:11 | WARNING  | pipeline.cleaning | Source 'customers': quarantined 6 of 18 row(s)
```

---

## Configuration reference

The whole pipeline is described by one YAML file. No Python changes are needed to add a column, change a rule or point at a different file.

### `pipeline`

| Key | Default | Meaning |
|---|---|---|
| `name` | `pipeline` | label used in logs and the report |
| `output_dir` | `data/output` | where outputs are written |
| `quarantine_rejects` | `true` | write rejected rows to `rejected_rows.csv` |
| `max_reject_ratio` | `1.0` | fail the run if more than this fraction of rows are rejected |
| `fail_fast` | `false` | stop at the first error |

### `sources`

Each source needs a `name`, a `type` (`csv`, `json` or `api`), a location (`path` or `url`) and a `schema`.

```yaml
sources:
  - name: customers
    type: csv
    path: data/input/customers.csv
    options:
      delimiter: ","
      encoding: utf-8-sig
    schema:
      dedupe_on: [customer_id]
      null_tokens: ["", "na", "n/a", "null", "-", "unknown"]
      drop_unknown_columns: true
      fields:
        - name: customer_id
          type: string
          required: true
          transform: [strip, upper]
```

### Field options

| Key | Meaning |
|---|---|
| `name` | column name in the output |
| `source_name` | column name in the input, if different |
| `type` | `string`, `integer`, `float`, `boolean`, `date`, `datetime`, `category` |
| `required` | if true, a missing or unparseable value quarantines the row |
| `default` | value used when `fill: default` |
| `fill` | `none`, `default`, `mean`, `median`, `mode`, `zero`, `empty`, `ffill` |
| `transform` | `strip`, `lower`, `upper`, `title`, `collapse_spaces`, `strip_currency`, `digits_only` |
| `allowed` | list of permitted values |
| `min` / `max` | numeric bounds |
| `date_format` | explicit `strftime` pattern, tried before the built-in formats |

A field is rejected rather than guessed. If `quantity` is declared as an integer and the value is `"two"`, the row goes to quarantine — it does not silently become null or zero.

### `transforms`

Steps run in order. Each reads named datasets and writes a named dataset back, so later steps can build on earlier ones.

| Type | Purpose |
|---|---|
| `join` | merge two datasets on a key (`left`, `right`, `inner`, `outer`) |
| `derive_arithmetic` | new column from `+`, `-`, `*`, `/` on columns or literals |
| `derive_date_part` | extract `year`, `month`, `quarter`, `weekday`, `year_month`, ... |
| `map_values` | recode values via a lookup, with a default |
| `filter_rows` | keep rows matching `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_null` |
| `aggregate` | group and apply `sum`, `mean`, `count`, `nunique`, `min`, `max`, ... |

```yaml
transforms:
  - type: derive_arithmetic
    dataset: orders
    target: line_total
    left: quantity
    op: "*"
    right: unit_price
    round: 2

  - type: aggregate
    dataset: customer_orders
    name: customer_summary
    group_by: [customer_id, full_name, country]
    sort_by: total_spend
    ascending: false
    aggregations:
      - { column: line_total, func: sum, target: total_spend, round: 2 }
      - { column: order_id, func: nunique, target: order_count }
```

### `outputs`

```yaml
outputs:
  - { dataset: customers,       filename: customers_clean.csv, format: csv }
  - { dataset: monthly_revenue, filename: monthly_revenue.json, format: json }
```

Formats: `csv`, `json`, `jsonl`.

---

## Reading from an API

`config.api.example.yaml` shows a working API source. The reader handles timeouts, retries with exponential backoff on `429` and `5xx`, and pagination.

```yaml
sources:
  - name: posts
    type: api
    url: https://jsonplaceholder.typicode.com/posts
    options:
      timeout: 15
      max_retries: 3
      backoff_seconds: 1.0
      max_pages: 1
      page_param: _page
      auth_env: API_TOKEN     # NAME of an env var, never the token itself
      params:
        _limit: 25
```

Tokens are never stored in the config. `auth_env` names an environment variable, and the reader reads it at run time and sends it as a bearer token:

```bash
export API_TOKEN="your-token-here"
python -m pipeline --config config.api.example.yaml
```

This example needs network access, so it is not run in CI.

---

## Project structure

```
data-pipeline/
├── pipeline/
│   ├── __init__.py          public API
│   ├── __main__.py          enables `python -m pipeline`
│   ├── cli.py               argument parsing and exit codes
│   ├── config.py            YAML parsing, env overrides, validation
│   ├── logging_setup.py     console and rotating file handlers
│   ├── readers.py           CSV, JSON/NDJSON and HTTP API input
│   ├── cleaning.py          nulls, type coercion, validation, dedupe
│   ├── transform.py         join, derive, map, filter, aggregate
│   ├── writers.py           atomic CSV/JSON/JSONL output and run report
│   └── pipeline.py          orchestration
├── tests/
│   └── test_pipeline.py     75 tests
├── data/
│   ├── input/               sample messy CSV and JSON
│   └── output/              generated
├── config.yaml              main configuration
├── config.api.example.yaml  API source example
├── requirements.txt
├── requirements-dev.txt
├── Makefile
└── .github/workflows/tests.yml
```

---

## Design notes

**Configuration over code.** Adding a column, changing a validation rule or pointing at a different file is a YAML edit. The Python never changes. That is what makes the same codebase usable for a different dataset.

**Bad rows should not kill a run.** A single malformed row in a 500,000-row file should not lose the other 499,999. Rows that cannot be repaired are quarantined with a reason and the run continues. `max_reject_ratio` is the safety net: if too much of the file is unusable, something is wrong upstream and the run reports an error.

**Fail loudly on ambiguity, quietly on the expected.** `N/A` in a numeric column is expected, so it becomes null and is filled per the declared strategy. `"two"` in an integer column is ambiguous, so the row is rejected rather than guessed at. Guessing is how silently wrong numbers reach a dashboard.

**Config must not be able to execute code.** An obvious way to support derived columns is to `eval()` an expression from the YAML. That would mean anyone who can edit the config can run arbitrary Python. The transforms are a fixed, auditable set of operations instead.

**Validate the config before touching data.** The whole config is parsed into typed dataclasses and checked up front, so a typo fails in milliseconds with a clear message rather than halfway through a long run.

**Writes are atomic.** Output goes to a temporary file in the target directory and is then moved into place, so a crash mid-write leaves the previous output intact rather than a truncated file that still looks valid.

**One bug worth recording.** pandas silently converts a mapped `None` into a float `NaN`. An early version checked `if value is None` before doing string work, which the `NaN` slipped past — so `str(nan)` turned missing values into the literal text `"nan"`, and a blank name was title-cased into `"Nan"` and passed validation. The fix was a single `is_null()` helper used at every value-level check. There is a regression test named after it in `TestNulls`.

---

## Testing

```bash
python -m pytest tests/ -v
```

```
75 passed in 0.76s
```

Covered: config validation and env overrides; encoding, BOM, empty-file, duplicate-header, nested-JSON, NDJSON and malformed-JSON reading; every type coercion and fill strategy; required, range and allowed-value rejection; deduplication; each transform plus its error cases; all three output formats and atomic writes; and four end-to-end runs against the real sample data.

CI runs the suite on Python 3.10, 3.11 and 3.12, then runs the pipeline end to end.

---

## Licence

MIT
