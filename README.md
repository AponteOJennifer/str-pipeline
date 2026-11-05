# Short-Term Rental Pricing & Demand Pipeline

An orchestrated pipeline that ingests Inside Airbnb data, models it into analytics
tables, and estimates occupancy, average daily rate, and RevPAR by neighborhood and
season for a short-term rental market.

> **Status:** weekend 1 of 3. Ingestion and orchestration are in place. dbt models and
> the forecast come next.

---

## Why this exists

Hotel revenue management has a standard metric set: occupancy, ADR (average daily
rate), and RevPAR (revenue per available room). Short-term rental data is rarely
analyzed that way, partly because the public data has no bookings in it.

It does have reviews. Reviews are a well-documented proxy for stays, which makes an
occupancy estimate possible, and from there ADR and RevPAR follow. This project
applies hotel revenue management metrics to a short-term rental market.

---

## Architecture

```
Inside Airbnb (gzipped CSV, per city, per snapshot date)
        │
        ▼
   Airflow DAG  ──  download → stage (Parquet) → merge (idempotent) → row-count check
        │
        ▼
     DuckDB  (raw_listings, raw_reviews)
        │
        ▼
      dbt     ──  staging → intermediate → marts
        │
        ▼
  Occupancy / ADR / RevPAR by neighborhood and month
```

**Stack:** Python, Apache Airflow, DuckDB, dbt, pandas

---

## Design notes

**Idempotent loads.** The merge task deletes the `(city, snapshot_date)` partition
before inserting. Rerunning a date never duplicates rows, which matters because
backfills get rerun.

**Skip, don't fail, on missing snapshots.** Inside Airbnb publishes on an irregular
cadence, so most months have no file. A 404 raises `AirflowSkipException` rather than
an error, so a backfill over a two-year range completes instead of filling the UI
with red.

**Schema drift is logged, not fatal.** Column names have changed across years of
snapshots. Missing columns are logged and the run continues, so older snapshots still
load.

**Column pruning at stage time.** The raw listings file has roughly 75 columns, most
of them free text. Trimming before Parquet keeps the warehouse small enough to work
with locally.

---

## Running it

Requires Python 3.9+. On Windows, run this under WSL.

```bash
git clone <this repo> && cd str-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export STR_PROJECT_DIR="$PWD"

airflow standalone
```

The UI comes up at `http://localhost:8080`. The generated admin password is printed
in the terminal and also written to `.airflow/simple_auth_manager_passwords.json.generated`.

Unpause `insideairbnb_ingest` and it begins backfilling from the start date.

---

## Configuration

Set your city at the top of `dags/insideairbnb_ingest.py`:

```python
CITY_PATH = "united-states/tx/austin"
CITY_NAME = "austin"
```

Find the exact path on [insideairbnb.com/get-the-data](https://insideairbnb.com/get-the-data/):
right-click a `listings.csv.gz` link, copy the address, and take the segment between
the host and the snapshot date.

---

## Results

<!-- Fill this in once the backfill has run. Numbers a reader can check:
     - snapshots loaded and date range
     - listings and reviews row counts
     - how long a full backfill takes
     - what happened when you reran a loaded date (row count unchanged) -->

_To be filled in after the first backfill._

---

## What I would do differently at scale

<!-- Write this after weekend 3. Honest tradeoffs, not a feature list. Candidates:
     - DuckDB is right for a laptop and wrong for a team; what would change on Snowflake
     - snapshot-based sources vs. true incremental appends
     - where the occupancy estimate breaks down and what would validate it -->

_To be filled in._
