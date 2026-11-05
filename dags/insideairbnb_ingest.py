"""
Inside Airbnb ingestion.

Inside Airbnb publishes snapshots on an irregular cadence on specific days, not on
any fixed interval, so a cron schedule with catchup would request dates that mostly
do not exist. Instead this DAG takes an explicit list of snapshot dates and uses
dynamic task mapping to fan out one task per (snapshot, table) pair. Each pair gets
its own retries, so a single bad file does not take the run down with it.

Loads are idempotent: the merge step deletes the (city, snapshot_date) partition
before inserting, so rerunning a snapshot never duplicates rows.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import requests
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

CITY_PATH = "united-states/tx/austin"
CITY_NAME = "austin"

SNAPSHOTS = [
    "2025-12-22",
    "2026-03-25",
    "2026-06-22",
    "2026-09-17",
]

BASE_URL = "https://data.insideairbnb.com"
TABLES = ["listings", "reviews"]

PROJECT_DIR = os.environ.get("STR_PROJECT_DIR", os.path.expanduser("~/str-pipeline"))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
WAREHOUSE = os.path.join(DATA_DIR, "warehouse.duckdb")

LISTING_COLS = [
    "id", "host_id", "host_since", "host_is_superhost",
    "neighbourhood_cleansed", "latitude", "longitude",
    "property_type", "room_type", "accommodates", "bedrooms", "beds",
    "price", "minimum_nights", "maximum_nights",
    "availability_30", "availability_60", "availability_90", "availability_365",
    "number_of_reviews", "number_of_reviews_ltm", "first_review", "last_review",
    "review_scores_rating", "reviews_per_month",
]


def _partition_dir(snapshot: str) -> str:
    return os.path.join(DATA_DIR, "raw", f"city={CITY_NAME}", f"snapshot={snapshot}")


@dag(
    dag_id="insideairbnb_ingest",
    description="Ingest Inside Airbnb listings and reviews into DuckDB",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["str", "ingest"],
)
def insideairbnb_ingest():

    @task
    def plan() -> list[dict]:
        """One work item per (snapshot, table). Mapped tasks fan out from this."""
        items = [{"snapshot": s, "table": t} for s in SNAPSHOTS for t in TABLES]
        log.info("planning %s ingest tasks: %s", len(items), items)
        return items

    @task(max_active_tis_per_dag=2)
    def ingest(snapshot: str, table: str) -> dict:
        """Download one gzipped CSV, trim it, and merge it into DuckDB."""
        url = f"{BASE_URL}/{CITY_PATH}/{snapshot}/data/{table}.csv.gz"
        log.info("GET %s", url)

        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        log.info("downloaded %.1f MB", len(resp.content) / 1e6)

        out_dir = _partition_dir(snapshot)
        os.makedirs(out_dir, exist_ok=True)

        raw_path = os.path.join(out_dir, f"{table}.csv.gz")
        with open(raw_path, "wb") as fh:
            fh.write(resp.content)

        with gzip.open(raw_path, "rb") as fh:
            df = pd.read_csv(io.BytesIO(fh.read()), low_memory=False)
        log.info("parsed %s rows, %s columns", len(df), len(df.columns))

        if table == "listings":
            keep = [c for c in LISTING_COLS if c in df.columns]
            missing = sorted(set(LISTING_COLS) - set(keep))
            if missing:
                log.warning("snapshot %s missing columns: %s", snapshot, missing)
            df = df[keep]
            if "price" in df.columns:
                df["price"] = (
                    df["price"].astype(str)
                    .str.replace(r"[$,]", "", regex=True)
                    .pipe(pd.to_numeric, errors="coerce")
                )

        df["city"] = CITY_NAME
        df["snapshot_date"] = snapshot

        parquet_path = os.path.join(out_dir, f"{table}.parquet")
        df.to_parquet(parquet_path, index=False)

        os.makedirs(DATA_DIR, exist_ok=True)
        con = duckdb.connect(WAREHOUSE)
        try:
            tbl = f"raw_{table}"
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {tbl} AS "
                f"SELECT * FROM read_parquet(?) LIMIT 0",
                [parquet_path],
            )
            con.execute(
                f"DELETE FROM {tbl} WHERE city = ? AND snapshot_date = ?",
                [CITY_NAME, snapshot],
            )
            con.execute(
                f"INSERT INTO {tbl} SELECT * FROM read_parquet(?)",
                [parquet_path],
            )
            rows = con.execute(
                f"SELECT count(*) FROM {tbl} WHERE city = ? AND snapshot_date = ?",
                [CITY_NAME, snapshot],
            ).fetchone()[0]
        finally:
            con.close()

        log.info("merged %s rows into raw_%s for %s", rows, table, snapshot)
        return {"snapshot": snapshot, "table": table, "rows": rows}

    @task
    def check(results: list[dict]) -> None:
        """Fail the run if any partition came back implausibly small."""
        floor = 100
        bad = [r for r in results if r["rows"] < floor]
        for r in results:
            log.info("%s / %s: %s rows", r["snapshot"], r["table"], r["rows"])
        if bad:
            raise ValueError(f"partitions below {floor} rows: {bad}")
        log.info("row count check passed for %s partitions", len(results))

    check(ingest.expand_kwargs(plan()))


insideairbnb_ingest()
