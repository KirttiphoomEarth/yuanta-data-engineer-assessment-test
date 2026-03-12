# Yuanta Brokerage Data Pipeline

A containerised data pipeline that ingests daily brokerage CSV drops, applies data-quality rules, and loads clean data into a queryable SQLite database.

---

## Repository Structure

```
.
├── dags/
│   ├── pipeline_initial.py        # Step 1 – create framework log tables
│   └── pipeline_transformation.py # Step 2 – clean & load all source files
├── input/
│   ├── clients.csv
│   ├── instruments.csv
│   └── trades_2026-03-09.csv
├── docker-compose.yml
└── README.md
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)

No other local dependencies are required.

---

## How to Start Everything

```bash
# 1. Clone the repository
git clone <repo-url>
cd yuanta-data-engineer-assessment-test

# 2. Start all services (Airflow + SQLite + SQLite Web viewer)
docker compose up -d

# 3. Wait ~60 s for Airflow to finish its first-time setup, then open:
#    Airflow UI  → http://localhost:8080  (user: admin / pass: admin)
#    DB viewer   → http://localhost:8081
```

### Default Credentials

| Service | URL | Username | Password |
|---|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` | `admin` |
| SQLite Web viewer | http://localhost:8081 | — | — |

> **Re-running from scratch**
> If you need a clean slate (e.g. after a failed first run), destroy the shared volume before restarting:
> ```bash
> docker compose down -v
> docker compose up -d
> ```

---

## Pipeline Overview

The pipeline is split into two DAGs that must be run in order:

```
┌─────────────────────┐        ┌───────────────────────────────────────────┐
│  pipeline_initial   │        │         pipeline_transformation            │
│  (run once first)   │──────► │                                            │
│                     │        │  transform_clients ──┐                     │
│  fw_transform_log   │        │                      ├──► transform_trades  │
│  fw_data_qulity_log │        │  transform_instru.. ─┘        │            │
└─────────────────────┘        │                       run_data_quality_checks│
                               └───────────────────────────────────────────┘
```

---

## Step 1 — `pipeline_initial` DAG

### Purpose

Creates the two framework log tables that every subsequent DAG writes to. **Must be run once before `pipeline_transformation`.**

### How to Trigger

1. Open the Airflow UI at `http://localhost:8080`.
2. Find the DAG **`pipeline_initial`** (tag: `framework / init`).
3. Toggle it **on** (unpause), then click **▶ Trigger DAG**.

### Task Graph

```
create_fw_transform_log  ──►  create_fw_data_quality_log
```

Both tasks use `CREATE TABLE IF NOT EXISTS` — fully idempotent, safe to re-run.

---

## Step 2 — `pipeline_transformation` DAG

### Purpose

Reads all source CSVs, applies the full set of cleaning rules identified during EDA, loads clean data into target tables, and writes DQ check results.

### Schedule

Runs automatically every day at **06:00 UTC** (`0 6 * * *`), after the nightly CSV drop is expected to land in the `input/` folder.

```
┌─ minute (0)
│ ┌─ hour (6 = 06:00 UTC)
│ │ ┌─ day of month (*)
│ │ │ ┌─ month (*)
│ │ │ │ ┌─ day of week (*)
0 6 * * *
```

### How to Trigger Manually

1. Open the Airflow UI at `http://localhost:8080`.
2. Find the DAG **`pipeline_transformation`** (tag: `pipeline / transformation`).
3. Toggle it **on**, then click **▶ Trigger DAG**.

> Dropping a new `trades_YYYY-MM-DD.csv` into `input/` and waiting for the next 06:00 UTC run is enough for periodic operation — no manual intervention needed. All tasks are idempotent (full-refresh), so re-triggering is always safe.

### Task Graph

```
transform_clients ──┐
                    ├──► transform_trades ──► run_data_quality_checks
transform_instruments ──┘
```

`transform_clients` and `transform_instruments` run in parallel. `transform_trades` waits for both because it validates foreign keys against the already-loaded dimension tables.

### What Each Task Does

| Task | Target table(s) | Key actions |
|---|---|---|
| `transform_clients` | `dim_clients` | Strip whitespace · UPPER client_id/kyc_status · NULL country kept |
| `transform_instruments` | `dim_instruments` | Strip whitespace · UPPER all string fields |
| `transform_trades` | `fact_trades`, `trades_rejected` | Normalise · dedup · strip comma numerics · apply reject rules |
| `run_data_quality_checks` | `fw_data_quality_log` | Evaluate all 13 EDA rules · write PASS/FAIL/WARNING per rule |

### Trades Cleaning Sequence

1. **Normalise** — strip whitespace, UPPER `client_id`, `instrument_id`, `side`, `status`
2. **Comma numerics** — strip `,` from `price`, `quantity`, `fees` and cast to `REAL`
3. **Deduplicate** — keep row with latest `trade_time` per `trade_id` (handles exact duplicates and late amendments)
4. **Default fees** — set `fees = 0.0` for `CANCELLED` trades with missing fees
5. **Reject rules** — evaluated in priority order; each trade is quarantined at most once (first rule that fires)

### Reject Rules (rows go to `trades_rejected`)

| Rule | Condition |
|---|---|
| `INVALID_FK_CLIENT` | `client_id` not in `dim_clients` |
| `INVALID_FK_INSTRUMENT` | `instrument_id` not in `dim_instruments` |
| `INVALID_SIDE` | `side` not in `{BUY, SELL}` |
| `NEGATIVE_PRICE` | `price < 0` |
| `ZERO_QUANTITY` | `quantity = 0` |
| `MISSING_PRICE` | `price IS NULL` |
| `MISSING_FEES` | `fees IS NULL` on a non-CANCELLED trade |

---

## Database Tables

### Target tables

| Table | Description |
|---|---|
| `dim_clients` | Cleaned client master data |
| `dim_instruments` | Cleaned instrument master data |
| `fact_trades` | Clean, validated trade events |
| `trades_rejected` | Quarantined rows with reject reason |

### Framework log tables

| Table | Description |
|---|---|
| `fw_transform_log` | Execution lifecycle per task (start, end, status, rows) |
| `fw_data_quality_log` | One row per DQ rule per run (checked, failed, status, affected keys) |

---

## Confirm Results

### Check all tables exist

```bash
docker compose exec db sqlite3 /db/brokerage.db ".tables"
```

Expected output:
```
dim_clients          dim_instruments      fact_trades
fw_data_quality_log  fw_transform_log     trades_rejected
```

### Verify pipeline ran successfully

```sql
SELECT dag_id, task_id, table_name, status, rows_processed, start_date, end_date
FROM   fw_transform_log
ORDER  BY start_date DESC;
```

### See clean trades loaded

```sql
SELECT COUNT(*) AS clean_trades FROM fact_trades;

SELECT * FROM fact_trades LIMIT 10;
```

### See quarantined rows

```sql
SELECT reject_reason, COUNT(*) AS cnt
FROM   trades_rejected
GROUP  BY reject_reason
ORDER  BY cnt DESC;
```

### See DQ results from last run

```sql
-- All rules summary
SELECT table_name, rule_code, status, rows_checked, rows_failed, failed_keys
FROM   fw_data_quality_log
ORDER  BY check_date DESC, table_name, rule_code;

-- Failed rules only
SELECT table_name, rule_code, rows_failed, failed_keys
FROM   fw_data_quality_log
WHERE  status = 'FAIL'
ORDER  BY check_date DESC;
```

### Browse data visually

Open `http://localhost:8081` — the SQLite Web viewer gives a table browser and SQL console against `brokerage.db`.

---

## DQ Rule Catalogue

| `rule_code` | Table | Status if triggered | Action |
|---|---|---|---|
| `MISSING_COUNTRY` | dim_clients | WARNING | Loaded as NULL; flagged for enrichment |
| `NON_APPROVED_KYC` | dim_clients | WARNING | Client loaded; trades not auto-rejected |
| `WHITESPACE_CASING` | fact_trades | WARNING | Fixed in-place (trim + upper) |
| `COMMA_NUMERIC` | fact_trades | WARNING | Fixed in-place (strip commas, cast to REAL) |
| `DUPLICATE_TRADE_ID` | fact_trades | WARNING | Kept latest trade_time row, dropped earlier |
| `LATE_UPDATE_TRADE_ID` | fact_trades | WARNING | Kept latest trade_time row (amendment) |
| `INVALID_FK_CLIENT` | fact_trades | FAIL | Quarantined to trades_rejected |
| `INVALID_FK_INSTRUMENT` | fact_trades | FAIL | Quarantined to trades_rejected |
| `INVALID_SIDE` | fact_trades | FAIL | Quarantined to trades_rejected |
| `NEGATIVE_PRICE` | fact_trades | FAIL | Quarantined to trades_rejected |
| `ZERO_QUANTITY` | fact_trades | FAIL | Quarantined to trades_rejected |
| `MISSING_PRICE` | fact_trades | FAIL | Quarantined to trades_rejected |
| `MISSING_FEES` | fact_trades | FAIL | Quarantined to trades_rejected |

---

## Design Decisions

| Decision | Rationale |
|---|---|
| **SQLite as target DB** | Zero-infrastructure overhead; sufficient for the assessment scope and reviewers running locally. |
| **WAL journal mode** | Set by the `db` container on startup so concurrent reads (sqlite-web) do not block pipeline writes. |
| **Full-refresh per run** | Each transformation task `DELETE`s and re-inserts, making every run idempotent without needing MERGE/UPSERT logic. |
| **clients & instruments in parallel** | They are independent; parallelism reduces total DAG runtime. |
| **trades waits for both dim tables** | FK validation requires dim tables to be fully loaded first. |
| **Reject-first-rule-wins** | A trade is quarantined once under the highest-priority rule; avoids duplicate rows in `trades_rejected`. |
| **WARNING vs FAIL in DQ** | Rows fixed in-place (whitespace, commas, duplicates) are WARNING — visible but not blocking. Rows quarantined are FAIL. |
| **`trades_*.csv` glob pattern** | Supports multiple daily drops without DAG changes. |
