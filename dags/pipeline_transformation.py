"""
pipeline_transformation.py
---------------------------
Cleans and loads the three brokerage source files into brokerage.db.

Task graph
----------
transform_clients ──┐
                    ├──► transform_trades ──► run_data_quality_checks
transform_instruments ──┘

transform_clients / transform_instruments
  - Read CSV from /opt/airflow/input/
  - Strip whitespace, normalise casing
  - Load into dim_clients / dim_instruments (full-refresh, idempotent)
  - Write result to fw_transform_log

transform_trades (depends on both dim tables for FK validation)
  - Read all trades_*.csv files
  - Normalise whitespace/casing, strip comma thousands separators
  - Deduplicate by trade_id (keep latest trade_time — handles exact dups + amendments)
  - Apply reject rules → quarantine bad rows to trades_rejected
  - Load clean rows into fact_trades
  - Write result to fw_transform_log

run_data_quality_checks
  - Re-reads raw CSVs + loaded tables to evaluate every EDA rule
  - Writes one row per rule per run to fw_data_quality_log
  - Status: PASS | FAIL | WARNING
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DB_PATH   = "/opt/airflow/db/brokerage.db"
INPUT_DIR = Path("/opt/airflow/input")

VALID_SIDES = {"BUY", "SELL"}

# Order matters: first matching rule wins for a given trade row
REJECT_RULES: list[tuple[str, str]] = [
    ("INVALID_FK_CLIENT",     "client_id not found in dim_clients"),
    ("INVALID_FK_INSTRUMENT", "instrument_id not found in dim_instruments"),
    ("INVALID_SIDE",          "side is not BUY or SELL"),
    ("NEGATIVE_PRICE",        "price is negative"),
    ("ZERO_QUANTITY",         "quantity equals zero"),
    ("MISSING_PRICE",         "price is NULL"),
    ("MISSING_FEES",          "fees is NULL (non-CANCELLED trade)"),
]

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def _write_transform_log(
    conn: sqlite3.Connection,
    dag_id: str, run_id: str, task_id: str, table_name: str,
    start_dt: str, end_dt: str,
    status: str, message: str | None, rows: int,
) -> None:
    conn.execute(
        """
        INSERT INTO fw_transform_log
               (dag_id, run_id, task_id, table_name,
                start_date, end_date, status, message, rows_processed, update_date)
        VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
        """,
        (dag_id, run_id, task_id, table_name,
         start_dt, end_dt, status, message, rows),
    )
    conn.commit()


def _write_dq_log(
    conn: sqlite3.Connection,
    dag_id: str, run_id: str, task_id: str, table_name: str,
    rule_code: str, rule_desc: str,
    rows_checked: int, rows_failed: int,
    status: str,
    failed_keys: list | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO fw_data_quality_log
               (dag_id, run_id, task_id, table_name,
                rule_code, rule_description,
                check_date, rows_checked, rows_failed, status, failed_keys, update_date)
        VALUES (?,?,?,?,?,?,datetime('now'),?,?,?,?,datetime('now'))
        """,
        (dag_id, run_id, task_id, table_name,
         rule_code, rule_desc,
         rows_checked, rows_failed, status,
         json.dumps(failed_keys) if failed_keys else None),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Task: transform_clients
# ---------------------------------------------------------------------------

def transform_clients(**context: dict) -> None:
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id
    start   = datetime.utcnow().isoformat()

    db = _conn()
    try:
        # ── create target table ──────────────────────────────────────────────
        db.execute("""
            CREATE TABLE IF NOT EXISTS dim_clients (
                client_id   TEXT PRIMARY KEY,
                client_name TEXT,
                country     TEXT,       -- NULL allowed; flag for enrichment
                kyc_status  TEXT,
                created_at  TEXT,
                loaded_at   DATETIME DEFAULT (datetime('now'))
            )
        """)
        db.execute("DELETE FROM dim_clients")

        # ── read ─────────────────────────────────────────────────────────────
        df = pd.read_csv(INPUT_DIR / "clients.csv")

        # ── clean ────────────────────────────────────────────────────────────
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].str.strip()

        df["client_id"]  = df["client_id"].str.upper()
        df["kyc_status"] = df["kyc_status"].str.upper()
        # country stays as-is (NaN → NULL in DB; flagged by DQ check)

        # ── load ─────────────────────────────────────────────────────────────
        df.to_sql("dim_clients", db, if_exists="append", index=False)

        end = datetime.utcnow().isoformat()
        _write_transform_log(db, dag_id, run_id, task_id, "dim_clients",
                             start, end, "SUCCESS", None, len(df))
        print(f"[transform_clients] loaded {len(df)} rows into dim_clients")

    except Exception as exc:
        end = datetime.utcnow().isoformat()
        _write_transform_log(db, dag_id, run_id, task_id, "dim_clients",
                             start, end, "FAILED", str(exc), 0)
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Task: transform_instruments
# ---------------------------------------------------------------------------

def transform_instruments(**context: dict) -> None:
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id
    start   = datetime.utcnow().isoformat()

    db = _conn()
    try:
        # ── create target table ──────────────────────────────────────────────
        db.execute("""
            CREATE TABLE IF NOT EXISTS dim_instruments (
                instrument_id TEXT PRIMARY KEY,
                symbol        TEXT,
                asset_class   TEXT,
                currency      TEXT,
                exchange      TEXT,
                loaded_at     DATETIME DEFAULT (datetime('now'))
            )
        """)
        db.execute("DELETE FROM dim_instruments")

        # ── read ─────────────────────────────────────────────────────────────
        df = pd.read_csv(INPUT_DIR / "instruments.csv")

        # ── clean ────────────────────────────────────────────────────────────
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].str.strip()

        df["instrument_id"] = df["instrument_id"].str.upper()
        df["asset_class"]   = df["asset_class"].str.upper()
        df["currency"]      = df["currency"].str.upper()
        df["exchange"]      = df["exchange"].str.upper()

        # ── load ─────────────────────────────────────────────────────────────
        df.to_sql("dim_instruments", db, if_exists="append", index=False)

        end = datetime.utcnow().isoformat()
        _write_transform_log(db, dag_id, run_id, task_id, "dim_instruments",
                             start, end, "SUCCESS", None, len(df))
        print(f"[transform_instruments] loaded {len(df)} rows into dim_instruments")

    except Exception as exc:
        end = datetime.utcnow().isoformat()
        _write_transform_log(db, dag_id, run_id, task_id, "dim_instruments",
                             start, end, "FAILED", str(exc), 0)
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Task: transform_trades
# ---------------------------------------------------------------------------

def transform_trades(**context: dict) -> None:
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id
    start   = datetime.utcnow().isoformat()

    db = _conn()
    try:
        # ── create target tables ─────────────────────────────────────────────
        db.execute("""
            CREATE TABLE IF NOT EXISTS fact_trades (
                trade_id      TEXT PRIMARY KEY,
                trade_time    DATETIME,
                client_id     TEXT,
                instrument_id TEXT,
                side          TEXT,
                quantity      REAL,
                price         REAL,
                fees          REAL,
                status        TEXT,
                loaded_at     DATETIME DEFAULT (datetime('now'))
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS trades_rejected (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id      TEXT,
                trade_time    DATETIME,
                client_id     TEXT,
                instrument_id TEXT,
                side          TEXT,
                quantity      REAL,
                price         REAL,
                fees          REAL,
                status        TEXT,
                reject_reason TEXT,
                rejected_at   DATETIME DEFAULT (datetime('now'))
            )
        """)
        db.execute("DELETE FROM fact_trades")
        db.execute("DELETE FROM trades_rejected")

        # ── load valid FK sets from already-loaded dim tables ────────────────
        valid_clients     = {r[0] for r in db.execute("SELECT client_id FROM dim_clients")}
        valid_instruments = {r[0] for r in db.execute("SELECT instrument_id FROM dim_instruments")}

        # ── read all trades_*.csv files ──────────────────────────────────────
        trade_files = sorted(INPUT_DIR.glob("trades_*.csv"))
        if not trade_files:
            raise FileNotFoundError(f"No trades_*.csv files found in {INPUT_DIR}")

        df = pd.concat(
            [pd.read_csv(f, dtype=str) for f in trade_files],
            ignore_index=True,
        )
        print(f"[transform_trades] raw rows loaded: {len(df)}")

        # ── step 1: normalise whitespace and casing ──────────────────────────
        for col in df.columns:
            df[col] = df[col].str.strip()

        df["client_id"]     = df["client_id"].str.upper()
        df["instrument_id"] = df["instrument_id"].str.upper()
        df["side"]          = df["side"].str.upper()
        df["status"]        = df["status"].str.upper()

        # ── step 2: strip comma thousands separators, cast to numeric ────────
        for col in ["quantity", "price", "fees"]:
            df[col] = pd.to_numeric(
                df[col].str.replace(",", "", regex=False), errors="coerce"
            )

        # ── step 3: deduplicate — keep latest trade_time per trade_id ────────
        # covers both exact duplicates (T0002) and late amendments (T0034)
        df["_trade_time_dt"] = pd.to_datetime(df["trade_time"], errors="coerce", utc=True)
        df = (
            df.sort_values("_trade_time_dt")
              .drop_duplicates(subset="trade_id", keep="last")
              .drop(columns=["_trade_time_dt"])
        )
        print(f"[transform_trades] rows after dedup: {len(df)}")

        # ── step 4: fix MISSING_FEES for CANCELLED trades (default 0.0) ──────
        cancelled_null_fees = df["fees"].isna() & (df["status"] == "CANCELLED")
        df.loc[cancelled_null_fees, "fees"] = 0.0

        # ── step 5: evaluate reject rules ────────────────────────────────────
        reject_masks: dict[str, pd.Series] = {
            "INVALID_FK_CLIENT":     ~df["client_id"].isin(valid_clients),
            "INVALID_FK_INSTRUMENT": ~df["instrument_id"].isin(valid_instruments),
            "INVALID_SIDE":          ~df["side"].isin(VALID_SIDES),
            "NEGATIVE_PRICE":        df["price"].notna() & (df["price"] < 0),
            "ZERO_QUANTITY":         df["quantity"].notna() & (df["quantity"] == 0),
            "MISSING_PRICE":         df["price"].isna(),
            "MISSING_FEES":          df["fees"].isna(),
        }

        # Each row goes to rejected at most once (first rule that fires)
        rejected_ids: set[str] = set()
        rejected_rows: list[pd.DataFrame] = []

        for rule_code, _ in REJECT_RULES:
            mask = reject_masks[rule_code]
            bad  = df[mask & ~df["trade_id"].isin(rejected_ids)].copy()
            if not bad.empty:
                bad["reject_reason"] = rule_code
                rejected_rows.append(bad)
                rejected_ids.update(bad["trade_id"].tolist())

        # ── step 6: split into clean vs rejected ─────────────────────────────
        clean_df    = df[~df["trade_id"].isin(rejected_ids)].copy()
        rejected_df = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

        print(f"[transform_trades] clean: {len(clean_df)}, rejected: {len(rejected_df)}")

        # ── step 7: load ──────────────────────────────────────────────────────
        trade_cols = [
            "trade_id", "trade_time", "client_id", "instrument_id",
            "side", "quantity", "price", "fees", "status",
        ]
        clean_df[trade_cols].to_sql("fact_trades", db, if_exists="append", index=False)

        if not rejected_df.empty:
            rej_cols = trade_cols + ["reject_reason"]
            rejected_df[rej_cols].to_sql(
                "trades_rejected", db, if_exists="append", index=False
            )

        end = datetime.utcnow().isoformat()
        _write_transform_log(
            db, dag_id, run_id, task_id, "fact_trades",
            start, end, "SUCCESS",
            f"clean={len(clean_df)}, rejected={len(rejected_df)}",
            len(clean_df),
        )

    except Exception as exc:
        end = datetime.utcnow().isoformat()
        _write_transform_log(db, dag_id, run_id, task_id, "fact_trades",
                             start, end, "FAILED", str(exc), 0)
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Task: run_data_quality_checks
# ---------------------------------------------------------------------------

def run_data_quality_checks(**context: dict) -> None:
    """
    Evaluate every EDA rule against the loaded tables.
    Writes one row per rule to fw_data_quality_log.

    Status mapping:
      PASS    – zero violations
      WARNING – violations present but intentionally kept (business rule flags)
      FAIL    – violations that caused rows to be quarantined / require action
    """
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id

    db = _conn()
    try:
        def _dq(
            table: str, rule_code: str, rule_desc: str,
            rows_checked: int, rows_failed: int,
            failed_keys: list | None = None,
            force_status: str | None = None,
        ) -> None:
            if force_status:
                status = force_status
            else:
                status = "PASS" if rows_failed == 0 else "FAIL"
            _write_dq_log(
                db, dag_id, run_id, task_id, table,
                rule_code, rule_desc,
                rows_checked, rows_failed, status, failed_keys,
            )
            print(f"[DQ] {table}.{rule_code}: {status} "
                  f"(checked={rows_checked}, failed={rows_failed})")

        # ── re-read raw CSVs for source-level checks ─────────────────────────
        raw_clients = pd.read_csv(INPUT_DIR / "clients.csv")
        for col in raw_clients.select_dtypes("object").columns:
            raw_clients[col] = raw_clients[col].str.strip()

        raw_trades_frames = [
            pd.read_csv(f, dtype=str) for f in sorted(INPUT_DIR.glob("trades_*.csv"))
        ]
        raw_trades = pd.concat(raw_trades_frames, ignore_index=True)
        for col in raw_trades.columns:
            raw_trades[col] = raw_trades[col].str.strip()

        total_raw_trades = len(raw_trades)

        # ── counts from loaded tables ─────────────────────────────────────────
        total_clients     = db.execute("SELECT COUNT(*) FROM dim_clients").fetchone()[0]
        total_instruments = db.execute("SELECT COUNT(*) FROM dim_instruments").fetchone()[0]
        total_clean       = db.execute("SELECT COUNT(*) FROM fact_trades").fetchone()[0]
        total_rejected    = db.execute("SELECT COUNT(*) FROM trades_rejected").fetchone()[0]

        # ================================================================
        # CLIENTS DQ
        # ================================================================

        # Rule 1: MISSING_COUNTRY
        missing_country = raw_clients[raw_clients["country"].isna()]["client_id"].tolist()
        _dq(
            "dim_clients", "MISSING_COUNTRY",
            "client.country is NULL — loaded as-is, flagged for enrichment",
            total_clients, len(missing_country), missing_country,
            force_status="WARNING" if missing_country else "PASS",
        )

        # Rule 2: NON_APPROVED_KYC (kept in dim_clients; flagged only)
        non_approved = raw_clients[
            raw_clients["kyc_status"].str.upper() != "APPROVED"
        ]["client_id"].tolist()
        _dq(
            "dim_clients", "NON_APPROVED_KYC",
            "kyc_status != APPROVED — client loaded; trades NOT rejected at pipeline level",
            total_clients, len(non_approved), non_approved,
            force_status="WARNING" if non_approved else "PASS",
        )

        # ================================================================
        # TRADES DQ — fix rules (rows corrected in-place, not rejected)
        # ================================================================

        # Rule 3: WHITESPACE_CASING — rows where client_id or side had whitespace/lowercase
        ws_mask = (
            raw_trades["client_id"].str.upper() != raw_trades["client_id"]
        ) | (
            raw_trades["side"].str.upper() != raw_trades["side"]
        )
        ws_ids = raw_trades[ws_mask]["trade_id"].tolist()
        _dq(
            "fact_trades", "WHITESPACE_CASING",
            "client_id / side had leading-trailing spaces or non-uppercase — fixed in place",
            total_raw_trades, len(ws_ids), ws_ids,
            force_status="WARNING" if ws_ids else "PASS",
        )

        # Rule 4: COMMA_NUMERIC — price / quantity / fees stored with comma separators
        def _has_comma(series: pd.Series) -> pd.Series:
            return series.fillna("").str.contains(r"\d,\d", regex=True)

        comma_mask = (
            _has_comma(raw_trades["price"])
            | _has_comma(raw_trades["quantity"])
            | _has_comma(raw_trades["fees"])
        )
        comma_ids = raw_trades[comma_mask]["trade_id"].tolist()
        _dq(
            "fact_trades", "COMMA_NUMERIC",
            "Numeric field had comma thousands separator — stripped and cast to REAL",
            total_raw_trades, len(comma_ids), comma_ids,
            force_status="WARNING" if comma_ids else "PASS",
        )

        # Rule 5: DUPLICATE_TRADE_ID — exact duplicates (same values except trade_time)
        dup_counts = raw_trades.groupby("trade_id").size()
        dup_ids_all = dup_counts[dup_counts > 1].index.tolist()
        # Exact dup: all non-time fields identical
        exact_dup_ids = []
        for tid in dup_ids_all:
            grp = raw_trades[raw_trades["trade_id"] == tid]
            non_time_cols = [c for c in grp.columns if c != "trade_time"]
            if grp[non_time_cols].duplicated(keep=False).all():
                exact_dup_ids.append(tid)
        _dq(
            "fact_trades", "DUPLICATE_TRADE_ID",
            "Exact duplicate trade_id rows — kept latest trade_time, dropped earlier",
            total_raw_trades, len(exact_dup_ids), exact_dup_ids,
            force_status="WARNING" if exact_dup_ids else "PASS",
        )

        # Rule 6: LATE_UPDATE_TRADE_ID — same trade_id, differing field values (amendment)
        late_ids = [t for t in dup_ids_all if t not in exact_dup_ids]
        _dq(
            "fact_trades", "LATE_UPDATE_TRADE_ID",
            "Same trade_id with amended values — kept row with latest trade_time",
            total_raw_trades, len(late_ids), late_ids,
            force_status="WARNING" if late_ids else "PASS",
        )

        # ================================================================
        # TRADES DQ — reject rules (rows quarantined to trades_rejected)
        # ================================================================

        for rule_code, rule_desc in REJECT_RULES:
            rows = db.execute(
                "SELECT trade_id FROM trades_rejected WHERE reject_reason = ?",
                (rule_code,),
            ).fetchall()
            rejected_ids_for_rule = [r[0] for r in rows]
            _dq(
                "fact_trades", rule_code, rule_desc,
                total_raw_trades, len(rejected_ids_for_rule), rejected_ids_for_rule,
            )

        # ── summary print ─────────────────────────────────────────────────────
        print(
            f"\n[DQ summary] clients={total_clients}, instruments={total_instruments}, "
            f"clean_trades={total_clean}, rejected_trades={total_rejected}"
        )

    finally:
        db.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="pipeline_transformation",
    description="Clean and load clients, instruments, and trades into brokerage.db.",
    start_date=datetime(2026, 3, 9),
    schedule="0 6 * * *",   # daily at 06:00 UTC — runs after nightly CSV drop
    catchup=False,
    tags=["pipeline", "transformation"],
) as dag:

    t_clients = PythonOperator(
        task_id="transform_clients",
        python_callable=transform_clients,
    )

    t_instruments = PythonOperator(
        task_id="transform_instruments",
        python_callable=transform_instruments,
    )

    t_trades = PythonOperator(
        task_id="transform_trades",
        python_callable=transform_trades,
    )

    t_dq = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    # clients & instruments can run in parallel; trades needs both FK sets
    [t_clients, t_instruments] >> t_trades >> t_dq
