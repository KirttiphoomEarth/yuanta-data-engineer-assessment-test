"""
run_local.py
------------
Run the full pipeline locally without Docker or Airflow.

Requirements:  pip install -r requirements-dev.txt

Usage:
    python run_local.py                    # run all steps, then open web UI
    python run_local.py --no-serve         # run all steps, print summary only
    python run_local.py --step init        # run only pipeline_initial
    python run_local.py --step transform   # run only pipeline_transformation
    python run_local.py --serve-only       # skip pipeline, just open web UI
    python run_local.py --port 8082        # use a custom port (default 8081)
"""

from __future__ import annotations

import argparse
import importlib.util
import sqlite3
import sys
import webbrowser
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Paths  (local equivalents of the Docker volume paths)
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).parent
LOCAL_DB    = str(BASE_DIR / "brokerage_local.db")
LOCAL_INPUT = BASE_DIR / "input"

# ---------------------------------------------------------------------------
# Mock Airflow — lets us import the DAG files without Airflow installed
# ---------------------------------------------------------------------------
def _mock_airflow() -> None:
    for mod in [
        "airflow",
        "airflow.models",
        "airflow.operators",
        "airflow.operators.python",
        "airflow.utils",
        "airflow.utils.dates",
    ]:
        sys.modules[mod] = MagicMock()

# ---------------------------------------------------------------------------
# Load a DAG module, then patch its path constants
# ---------------------------------------------------------------------------
def _load_module(name: str, filepath: Path) -> object:
    spec   = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# ---------------------------------------------------------------------------
# Fake Airflow task context
# ---------------------------------------------------------------------------
def _ctx(dag_id: str, task_id: str) -> dict:
    return {
        "dag":    SimpleNamespace(dag_id=dag_id),
        "run_id": f"local_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "task":   SimpleNamespace(task_id=task_id),
    }

# ---------------------------------------------------------------------------
# Step runners
# ---------------------------------------------------------------------------
def run_initial(init_mod: object) -> None:
    print("\n" + "="*60)
    print("STEP 1 — pipeline_initial")
    print("="*60)

    init_mod.DB_PATH = LOCAL_DB         # patch path before calling

    init_mod.create_fw_transform_log()
    init_mod.create_fw_data_quality_log()

    print("[DONE] fw_transform_log and fw_data_quality_log are ready.")


def run_transformation(trans_mod: object) -> None:
    print("\n" + "="*60)
    print("STEP 2 — pipeline_transformation")
    print("="*60)

    trans_mod.DB_PATH   = LOCAL_DB      # patch paths before calling
    trans_mod.INPUT_DIR = LOCAL_INPUT

    dag = "pipeline_transformation"

    print("\n[1/4] transform_clients ...")
    trans_mod.transform_clients(**_ctx(dag, "transform_clients"))

    print("\n[2/4] transform_instruments ...")
    trans_mod.transform_instruments(**_ctx(dag, "transform_instruments"))

    print("\n[3/4] transform_trades ...")
    trans_mod.transform_trades(**_ctx(dag, "transform_trades"))

    print("\n[4/4] run_data_quality_checks ...")
    trans_mod.run_data_quality_checks(**_ctx(dag, "run_data_quality_checks"))

    print("\n[DONE] All tasks complete.")


# ---------------------------------------------------------------------------
# Quick result summary
# ---------------------------------------------------------------------------
def print_summary() -> None:
    print("\n" + "="*60)
    print("RESULT SUMMARY")
    print("="*60)

    conn = sqlite3.connect(LOCAL_DB)
    queries = {
        "dim_clients rows":       "SELECT COUNT(*) FROM dim_clients",
        "dim_instruments rows":   "SELECT COUNT(*) FROM dim_instruments",
        "fact_trades (clean)":    "SELECT COUNT(*) FROM fact_trades",
        "trades_rejected":        "SELECT COUNT(*) FROM trades_rejected",
        "transform_log entries":  "SELECT COUNT(*) FROM fw_transform_log",
        "dq_log entries":         "SELECT COUNT(*) FROM fw_data_quality_log",
    }
    for label, sql in queries.items():
        try:
            count = conn.execute(sql).fetchone()[0]
            print(f"  {label:<30} {count}")
        except Exception:
            print(f"  {label:<30} (table not found)")

    print("\n  Rejected trades by reason:")
    try:
        for row in conn.execute(
            "SELECT reject_reason, COUNT(*) FROM trades_rejected GROUP BY reject_reason ORDER BY 2 DESC"
        ):
            print(f"    {row[0]:<35} {row[1]}")
    except Exception:
        print("    (trades_rejected not found)")

    print("\n  DQ check results:")
    try:
        for row in conn.execute(
            "SELECT table_name, rule_code, status, rows_failed FROM fw_data_quality_log ORDER BY table_name, rule_code"
        ):
            print(f"    [{row[2]:<7}] {row[0]}.{row[1]} — failed={row[3]}")
    except Exception:
        print("    (fw_data_quality_log not found)")

    conn.close()
    print(f"\n  Database file: {LOCAL_DB}")


# ---------------------------------------------------------------------------
# Web UI  (sqlite-web)
# ---------------------------------------------------------------------------
def serve_web_ui(port: int) -> None:
    """Launch sqlite-web against brokerage_local.db and open the browser."""
    try:
        from sqlite_web import run_server  # type: ignore
    except ImportError:
        print("\n[WARN] sqlite-web not installed — run: pip install sqlite-web")
        print(f"       Then manually: sqlite_web -p {port} {LOCAL_DB}")
        return

    url = f"http://127.0.0.1:{port}"
    print(f"\n{'='*60}")
    print(f"  Web UI  →  {url}")
    print(f"  Database → {LOCAL_DB}")
    print(f"  Press Ctrl+C to stop.")
    print(f"{'='*60}\n")

    webbrowser.open(url)
    run_server(LOCAL_DB, host="127.0.0.1", port=port, debug=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Run pipeline locally")
    parser.add_argument(
        "--step",
        choices=["init", "transform", "all"],
        default="all",
        help="Which step to run (default: all)",
    )
    parser.add_argument(
        "--no-serve",
        action="store_true",
        help="Skip launching the web UI after the pipeline finishes",
    )
    parser.add_argument(
        "--serve-only",
        action="store_true",
        help="Skip the pipeline, just open the web UI on the existing DB",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8081,
        help="Port for the sqlite-web UI (default: 8081)",
    )
    args = parser.parse_args()

    if args.serve_only:
        serve_web_ui(args.port)
        return

    _mock_airflow()

    dags_dir  = BASE_DIR / "dags"
    init_mod  = _load_module("pipeline_initial",       dags_dir / "pipeline_initial.py")
    trans_mod = _load_module("pipeline_transformation", dags_dir / "pipeline_transformation.py")

    if args.step in ("init", "all"):
        run_initial(init_mod)

    if args.step in ("transform", "all"):
        run_transformation(trans_mod)

    if args.step == "all":
        print_summary()

    if not args.no_serve:
        serve_web_ui(args.port)


if __name__ == "__main__":
    main()
