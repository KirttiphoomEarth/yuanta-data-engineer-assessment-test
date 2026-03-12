"""
pipeline_initial.py
-------------------
One-time initialisation DAG.

Tasks
-----
1. create_fw_transform_log    – framework table that every pipeline task writes
                                its start/end/status to.
2. create_fw_data_quality_log – framework table that every DQ rule check writes
                                its result to (rules derived from EDA notebook).

The DAG is set to run once (schedule=None) and is idempotent
(CREATE TABLE IF NOT EXISTS), so re-running is always safe.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DB_PATH = "/opt/airflow/db/brokerage.db"

# ---------------------------------------------------------------------------
# DDL statements
# ---------------------------------------------------------------------------

DDL_TRANSFORM_LOG = """
CREATE TABLE IF NOT EXISTS fw_transform_log (
    id              INTEGER  PRIMARY KEY AUTOINCREMENT,
    dag_id          TEXT     NOT NULL,
    run_id          TEXT     NOT NULL,
    task_id         TEXT     NOT NULL,
    table_name      TEXT     NOT NULL,
    start_date      DATETIME,
    end_date        DATETIME,
    status          TEXT     NOT NULL DEFAULT 'RUNNING',
                             -- RUNNING | SUCCESS | FAILED | SKIPPED
    message         TEXT,    -- error / info message
    rows_processed  INTEGER  DEFAULT 0,
    update_date     DATETIME NOT NULL DEFAULT (datetime('now'))
);
"""

DDL_DATA_QUALITY_LOG = """
CREATE TABLE IF NOT EXISTS fw_data_quality_log (
    id               INTEGER  PRIMARY KEY AUTOINCREMENT,
    dag_id           TEXT     NOT NULL,
    run_id           TEXT     NOT NULL,
    task_id          TEXT     NOT NULL,
    table_name       TEXT     NOT NULL,   -- clients | instruments | trades
    rule_code        TEXT     NOT NULL,
    -- Rule catalogue (from EDA):
    --   clients  : MISSING_COUNTRY, NON_APPROVED_KYC
    --   trades   : DUPLICATE_TRADE_ID, LATE_UPDATE_TRADE_ID,
    --              INVALID_FK_CLIENT, INVALID_FK_INSTRUMENT,
    --              WHITESPACE_CASING, INVALID_SIDE, NEGATIVE_PRICE,
    --              ZERO_QUANTITY, MISSING_PRICE, MISSING_FEES,
    --              COMMA_NUMERIC
    rule_description TEXT,
    check_date       DATETIME NOT NULL DEFAULT (datetime('now')),
    rows_checked     INTEGER  DEFAULT 0,
    rows_failed      INTEGER  DEFAULT 0,
    status           TEXT     NOT NULL DEFAULT 'PASS',
                              -- PASS | FAIL | WARNING
    failed_keys      TEXT,    -- JSON array of affected primary-key values
    update_date      DATETIME NOT NULL DEFAULT (datetime('now'))
);
"""

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _create_table(ddl: str, table_name: str) -> None:
    """Execute a single DDL statement against the brokerage SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(ddl)
        conn.commit()
        print(f"[OK] Table '{table_name}' is ready.")
    except Exception as exc:
        print(f"[ERROR] Failed to create table '{table_name}': {exc}")
        raise
    finally:
        conn.close()


def create_fw_transform_log() -> None:
    _create_table(DDL_TRANSFORM_LOG, "fw_transform_log")


def create_fw_data_quality_log() -> None:
    _create_table(DDL_DATA_QUALITY_LOG, "fw_data_quality_log")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="pipeline_initial",
    description="Create framework log tables in brokerage.db (idempotent init).",
    start_date=datetime(2026, 3, 9),
    schedule=None,           # triggered manually or by other DAGs
    catchup=False,
    tags=["framework", "init"],
) as dag:

    t1 = PythonOperator(
        task_id="create_fw_transform_log",
        python_callable=create_fw_transform_log,
    )

    t2 = PythonOperator(
        task_id="create_fw_data_quality_log",
        python_callable=create_fw_data_quality_log,
    )

    t1 >> t2
