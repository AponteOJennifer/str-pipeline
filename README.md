# Short-Term Rental Pricing & Demand Pipeline

[![ci](https://github.com/AponteOJennifer/str-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AponteOJennifer/str-pipeline/actions/workflows/ci.yml)

An Airflow and dbt pipeline that turns Inside Airbnb snapshots into the three
numbers hotel revenue management runs on, occupancy, ADR and RevPAR, and adds a
backtested weekly demand forecast on top of them.

**146,490 listing-months modelled · 24 tests on every build · demand forecast at
7.83% MAPE twelve weeks out, 34% better than a seasonal baseline**

The pipeline's more interesting output was what it found in its own inputs. Two
of the four source snapshots are defective in ways a row-count check passes
without complaint: one contains every listing and no prices at all, and another
is missing 2,533 listings that a listing-ID trace identifies as missed by the
scrape rather than delisted from the market. Both are now caught automatically.
[The findings are below](#results), along with the two modelling approaches the
backtest ruled out.

> **Status:** complete. Orchestrated ingestion, a tested dbt modelling
> layer, and a backtested weekly demand forecast written back to the
> warehouse.

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
Inside Airbnb  (gzipped CSV, per city, per snapshot date)
       │
       ▼
Airflow: insideairbnb_ingest
       download → stage (Parquet) → merge (idempotent) → row-count check
       │
       ▼
DuckDB   raw_listings, raw_reviews
       │
       ▼
dbt      staging → intermediate → marts
         snapshot completeness and rate-usability tests
       │
       ├──▶  Occupancy / ADR / RevPAR by neighbourhood and month
       │
       ▼
Airflow: demand_forecast
       backtest 7 models (rolling origin) → fit champion → forecast 12 weeks
       │
       ▼
DuckDB   forecast_weekly, forecast_backtest   (keyed by training vintage)
       │
       ▼
dbt      mart_forecast_accuracy, mart_forecast_vs_actual
```

**Stack:** Python, Apache Airflow, DuckDB, dbt, pandas, scikit-learn

---

## Design notes

**Idempotent loads.** The merge task deletes the `(city, snapshot_date)` partition
before inserting. Rerunning a date never duplicates rows, which matters because
backfills get rerun.

**An explicit snapshot list, not a schedule.** Inside Airbnb publishes on no
fixed cadence and on arbitrary days of the month, so a cron schedule with
catchup would request dates that mostly do not exist and almost every run would
404. The DAG takes an explicit list of snapshot dates instead and fans out one
task per (snapshot, table) pair using dynamic task mapping, so each file carries
its own retries and one bad download cannot take the run down with it.

**Schema drift is logged, not fatal.** Column names have changed across years of
snapshots. Missing columns are logged and the run continues, so older snapshots still
load.

**Column pruning at stage time.** The raw listings file has roughly 75 columns, most
of them free text. Trimming before Parquet keeps the warehouse small enough to work
with locally.

---

## Running it

Requires Python 3.12. Airflow 2.10 does not support 3.13 or later, so a newer
interpreter will fail to install it. On Windows, run this under WSL.

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

`insideairbnb_ingest` is triggered manually rather than scheduled, because the
source has no cadence to follow. Put the snapshot dates you want in `SNAPSHOTS`
at the top of the DAG and trigger it. Loads are idempotent, so re-triggering a
date that is already loaded leaves row counts unchanged.

---

## Running the forecast

dbt and Airflow have incompatible dependency pins, so dbt lives in its own
virtualenv. The forecast runs in Airflow's, which is where scikit-learn is
installed.

```bash
source .venv/bin/activate

python -m forecast.explore          # inspect the series before modelling it
python -m forecast.run_backtest     # score every model, write the charts
python -m forecast.run_forecast     # ship the champion, write to the warehouse
```

Reruns are safe. `forecast_weekly` and `forecast_backtest` are written
delete-then-insert on their keys, so running twice produces the same row counts.

Then refresh the models that read them:

```bash
cd dbt && export DBT_PROFILES_DIR=$PWD
../.venv-dbt/bin/dbt build --select source:forecast+
```

Or trigger the `demand_forecast` DAG, which does both in order.

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

Four Austin snapshots: 22 December 2025, 25 March 2026, 22 June 2026, and
17 September 2026. 41,827 listing-snapshot rows and 2,479,896 raw review rows,
deduplicated to one row per review ID. Reruns are safe: the merge deletes the
`(city, snapshot_date)` partition before inserting, so a reloaded snapshot
leaves row counts unchanged.

The analytics layer produces 146,490 listing-months spanning October 2023 to
September 2026. 22,963 of those carry a rate. That gap is the finding below.

### Two of the four snapshots are defective, in two different ways

**March 2026 is an incomplete scrape.** It returned 8,170 listings against a
market median of 11,181, a 27 percent shortfall. Tracing listing IDs across
snapshots, 2,533 listings present in both December and June are absent from
March: 84 percent of the 3,011-listing gap. A listing cannot leave the market
and come back, so those were missed rather than delisted. Left in, the snapshot
reads as a market collapse and recovery that never happened.

**December 2025 has every listing and no prices.** All 11,251 listings are
there; the price column is empty for every one of them. A row-count check
passes this snapshot without complaint.

These are different failures and they disqualify a snapshot for different
purposes, so the pipeline evaluates them separately. `is_complete` governs
anything that counts or sizes the market. `has_usable_rates` governs the rate
lookup. December is complete and unusable for rate; March is usable for rate
and incomplete. Collapsing the two into one flag is what let the December
snapshot anchor every rate in the warehouse and silently empty the fact table.

Both tests run on every build as dbt tests at `warn` severity, so a bad
snapshot announces itself rather than being discovered in a chart weeks later.

### Demand reproduces Austin's event calendar

Reviews as a stay proxy recover the city's real seasonality without any event
data as input:

| Month | Reviews | Month | Reviews |
|---|---|---|---|
| Jan 2025 | 8,126 | Jan 2026 | 9,115 |
| Mar 2025 | 15,228 | Mar 2026 | 16,603 |
| Oct 2025 | 16,437 | Jun 2026 | 12,869 |
| Dec 2025 | 9,426 | Aug 2026 | 12,309 |

March is SXSW. October is Austin City Limits. January is the trough in both
years. Peak to trough is roughly 2x, and the year over year comparison shows
about 10 percent growth on top of the seasonal pattern.

### Rate and RevPAR, April to August 2026

| Month | Median ADR | Median occupancy | Median RevPAR |
|---|---|---|---|
| Apr 2026 | $249.50 | 0.446 | $136.48 |
| May 2026 | $257.50 | 0.461 | $143.30 |
| Jun 2026 | $258.63 | 0.440 | $137.09 |
| Jul 2026 | $244.00 | 0.464 | $133.37 |
| Aug 2026 | $224.06 | 0.450 | $122.44 |

**Read the ADR column with care.** April, May and June all draw their price
from the same March snapshot, so no listing was repriced between them. The
median moves because the set of listings with reviews changes month to month.
This is a composition effect, not a pricing signal, and with three price
snapshots a monthly repricing series is not observable at all. The occupancy
and demand columns are not subject to this; they come from review dates
directly.

---

## Known limitations

**The occupancy estimator runs hot at the top end.** Occupancy is derived as
estimated stays times assumed length of stay, divided by days in month, capped
at 70 percent. High-review listings hit that ceiling, which compresses the
upper tail. The cap, the review rate, and the length of stay are all dbt vars,
so the sensitivity is testable, but none of them are calibrated against real
booking data because none exists in this source.

**Rate is a listed rate, not an achieved rate.** RevPAR here is an upper bound.
Discounts, cleaning fees and length-of-stay pricing are invisible.

**Demand before December 2025 is survivorship-biased.** Attributes come from
the snapshots, so a listing delisted before the first snapshot has reviews but
no neighbourhood and never enters the fact table. The further back the series
runs, the more it reflects only listings that survived into the snapshot
window. `demand_months` controls the depth of that tradeoff.

**Neighbourhoods are ZIP codes.** That is how Austin's Inside Airbnb data
publishes `neighbourhood_cleansed`, not a modelling choice.

---

## Forecasting demand

Demand is the half of this data with real history. Rate comes from three price
snapshots; reviews run for years. So the forecast targets demand, and it works
weekly rather than monthly: 193 weeks from January 2023 is enough to fit and
backtest where 35 monthly points would not be, and weekly aggregation sharpens
the festival peaks from a smeared monthly bump into something a model can learn.

![Weekly demand](analysis/demand_weekly.png)

### What gets measured

Seven models, refit at 12 origins stepped four weeks apart, each forecasting 12
weeks ahead: 144 scored predictions per model, 1,008 in total. Scored on MAE
and MAPE plus skill against seasonal naive on identical folds.

| Model | MAE | MAPE | Skill vs naive |
|---|---|---|---|
| `seasonal_naive_drift` | 235 | 7.83% | **+34.4%** |
| `gbm_seasonal_lags` | 255 | 8.73% | +28.7% |
| `seasonal_window_drift` | 275 | 9.58% | +23.0% |
| `gbm_seasonal_ratio` | 288 | 9.94% | +19.6% |
| `seasonal_naive` | 358 | 11.60% | baseline |
| `fourier_ridge_trend` | 408 | 14.26% | -14.1% |
| `fourier_ridge` | 714 | 22.72% | -99.6% |

The shipped model has two parameters: last year's same week, rescaled by the
ratio of the recent 13 weeks to the same 13 weeks a year earlier. It beats
gradient boosting.

![Forecast](analysis/demand_forecast.png)

### Three results that matter more than the winner

**A fitted trend is actively harmful here, and it is measurable.** Both Fourier
variants lose to seasonal naive. The cause is survivorship: reviews rise from 31
in 2009 to 146,569 in 2025, and almost none of that is market growth, because a
listing delisted before the first snapshot never appears in the data at all.
Annualise 2026 and it lands near 2025's total, so the market is close to flat. A
model with a free trend parameter reads the backwards decay as forwards growth.

Running the same backtest from 2022 instead of 2023 confirms it from the other
side: every model that uses the extra year gets worse (gradient boosting 8.73%
to 9.28%, Fourier with trend 14.26% to 16.10%, Fourier without 22.72% to
31.05%), while the two models that never look back more than a year are
unchanged to the decimal. More history made the fitted models worse. That is why
the training window starts in 2023, and it is a tested choice rather than a
preference.

**The error is concentrated, and its largest cluster has a specific cause.** Six
weeks out of 56 carry 34% of all absolute error. The largest pair:

```
2026-03-09   actual 3,676   forecast 4,756   29.4% too high
2026-03-16   actual 4,492   forecast 3,834   14.6% too low
```

SXSW peaked the week of 10 March in 2025 and 16 March in 2026. A year-ago
lookup aims one week off and misses twice: high on the week the festival left,
low on the week it moved to.

**The obvious fix for that does not pay for itself.** `seasonal_window_drift`
blends the three weeks around the year-ago reference to absorb a one-week shift.
On a synthetic series built to contain timing drift it wins, 4.94% against
5.38%, so the mechanism is real. On this data it loses, 9.58% against 7.83%. It
improves exactly the horizons predicted (horizon 7 from 8.49% to 7.71%, horizon
11 from 11.88% to 9.50%) and degrades the ordinary weeks holding the other 66%
of the error by more than it gains. The model stays in the repo because the
negative result is the evidence for shipping the simpler one.

![Error by horizon](analysis/backtest_by_horizon.png)

### Uncertainty

Intervals are empirical, taken from the distribution of actual over forecast
across all 144 backtest predictions: 80% at [0.87, 1.13], 95% at [0.80, 1.16].
They are pooled across horizons, and the run prints the diagnostic that
justifies pooling rather than assuming it: error growth is +0.002 log points per
week of horizon, which is flat. That flatness follows from anchoring on last
year rather than on recent momentum. If a future refresh changes it, the run
says so instead of quietly producing intervals that are too narrow three months
out.

### One-off events

April 2024 totals 15,650 reviews against March's 10,772, inverting the SXSW
pattern that holds in every other year, with the single busiest week of the year
falling on 8 April. That is the total solar eclipse, which put Austin in the path
of totality. Recurring events need no special handling, because the Fourier basis
carries anything date-anchored and SXSW and ACL land in roughly the same week
each year. A one-time shock is precisely what a periodic function cannot
represent, so it is the only entry in `forecast/events.csv`.

### Keeping the forecast accountable

`forecast_weekly` is keyed on `(model, trained_through, target_week)`. A rerun
replaces its own vintage; a run after a new snapshot lands adds a new one. Past
forecasts stay scoreable against what actually happened, which
`mart_forecast_vs_actual` does, including whether the outcome landed inside the
80% and 95% intervals. A forecast table holding only the current vintage can
never be evaluated, and a forecast nobody can score is not a forecast.

---

## What I would do differently at scale

**Ingest the calendar file.** Three price snapshots is the binding constraint
on this whole project, and it is the one limitation that is fixable from the
same source. `calendar.csv.gz` carries forward-looking nightly prices per
listing per date. That would replace an as-of join against three snapshots with
a real daily rate series, make month over month repricing observable, and let
availability be measured rather than inferred from reviews. It is also two
orders of magnitude more data, which is why it was out of scope here and why it
would be the first thing added.

**DuckDB is right for one analyst and wrong for a team.** It is a single file
with no concurrent writers, so two people cannot build at once and there is no
access control. The dbt layer would move to Snowflake or BigQuery unchanged
beyond the profile; the Airflow tasks would write Parquet to object storage and
`COPY INTO` rather than inserting locally. Nothing in the modelling would need
to change, which is the point of keeping the warehouse-specific code confined
to the ingest step.

**Snapshots are a bad source contract and I would stop pretending otherwise.**
Each file is a full restatement of the market on one arbitrary day. Everything
awkward in this project traces back to that: the as-of join, the completeness
tests, the partial trailing month. With an event stream the grain would be the
change, not the file, and the fact table would be incremental rather than a
full rebuild. Given snapshots, the right move is the one taken here, which is
to treat every snapshot as suspect until tested and to record which snapshot
each number came from, so a wrong figure can be traced back to the file that
produced it.

**Validating the occupancy estimate needs outside data.** The review-rate
assumption is the largest single source of error and nothing in this source can
check it. The honest test is a handful of real operators' booking data, or a
published benchmark such as AirDNA or STR, compared against the estimate for
the same market and period. Until that comparison exists, the occupancy
numbers are internally consistent and externally unverified, and should be
described that way.

**Forecast by neighbourhood, not just citywide.** The current forecast is one
series for Austin. A revenue manager prices a building, not a city, and the
neighbourhood panel already exists in `mart_neighbourhood_demand_monthly`.
Doing it properly means forecasting at both levels and reconciling them so the
parts sum to the whole, which is a real problem rather than a loop over ZIP
codes: thin cells are noisy, and an unreconciled set of local forecasts will
not add up to the citywide number anyone is held to.

**Get the event calendar right rather than inferring it.** The largest
remaining error cluster is festival timing drift, and the fix that works on
synthetic data loses on real data because it blurs every ordinary week to help
a handful. Aligning on actual published SXSW, ACL and Grand Prix dates would
target only the weeks that need it. That needs a maintained event calendar,
including future dates, which is exactly the kind of dependency worth being
explicit about before taking it on.
