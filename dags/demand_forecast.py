"""Demand forecast as an orchestrated step.

Separate from the ingest DAG on purpose. Ingestion is driven by Inside Airbnb
publishing a new snapshot, which happens on no fixed schedule. Forecasting is
driven by wanting a refreshed number, which is a different trigger, and
coupling them would mean either forecasting when there is nothing new or
waiting on a scrape to get a forecast.

The forecast runs as a subprocess rather than an in-process import. Two
reasons. The CLI is the contract that gets used by hand, so running the same
entry point here means the orchestrated path and the manual path cannot drift
apart. And the dbt step genuinely has to be a subprocess: dbt lives in its own
virtualenv because its dependency pins conflict with Airflow's, so the two
cannot share an interpreter.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

PROJECT_DIR = os.environ.get("STR_PROJECT_DIR", os.path.expanduser("~/str-pipeline"))
DBT_DIR = os.path.join(PROJECT_DIR, "dbt")
DBT_BIN = os.path.join(PROJECT_DIR, ".venv-dbt", "bin", "dbt")

HORIZON = 12
TRAIN_START = "2023-01-01"   # earlier weeks are distorted by survivorship; see series.py


def run(cmd: list[str], cwd: str, env: dict | None = None) -> str:
    """Run a command, stream its output into the task log, fail loudly."""
    merged = {**os.environ, **(env or {})}
    proc = subprocess.run(
        cmd, cwd=cwd, env=merged,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, check=False,
    )
    print(proc.stdout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd)} exited {proc.returncode}; output above"
        )
    return proc.stdout


@dag(
    dag_id="demand_forecast",
    description="Backtest, forecast weekly demand, and refresh the forecast marts",
    schedule=None,          # triggered when a refreshed forecast is wanted
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["str", "forecast"],
)
def demand_forecast():

    @task
    def forecast() -> str:
        """Backtest every model, fit the champion, write to the warehouse.

        Writes are keyed on (model, trained_through, target_week), so a rerun
        replaces its own vintage and a run after a new snapshot adds one.
        Neither duplicates rows.
        """
        out = run(
            [sys.executable, "-m", "forecast.run_forecast",
             "--horizon", str(HORIZON), "--start", TRAIN_START],
            cwd=PROJECT_DIR,
        )
        tail = [ln for ln in out.splitlines() if "wrote" in ln]
        return tail[0] if tail else "forecast complete"

    @task
    def build_forecast_marts() -> None:
        """Rebuild the dbt models that read the forecast tables.

        Scoped with a selector rather than a bare `dbt build`: this DAG changed
        the forecast tables and nothing else, so rebuilding the ingest-side
        models would be wasted work and would blur which run produced which
        table.
        """
        if not os.path.exists(DBT_BIN):
            raise RuntimeError(
                f"dbt not found at {DBT_BIN}. The dbt virtualenv is separate "
                f"from Airflow's because their dependency pins conflict; "
                f"create it with: python3.12 -m venv {PROJECT_DIR}/.venv-dbt "
                f"&& {PROJECT_DIR}/.venv-dbt/bin/pip install dbt-core dbt-duckdb"
            )
        run(
            [DBT_BIN, "build", "--select",
             "source:forecast+ mart_forecast_accuracy mart_forecast_vs_actual"],
            cwd=DBT_DIR,
            env={"DBT_PROFILES_DIR": DBT_DIR},
        )

    build_forecast_marts().set_upstream(forecast())


demand_forecast()
