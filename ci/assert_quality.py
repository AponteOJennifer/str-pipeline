"""Assert the data quality models still catch the defects CI seeds for them.

dbt's own tests check shape: uniqueness, not-null, accepted values. They would
all pass on a pipeline that had quietly stopped detecting a broken snapshot.
These assertions check behaviour instead. The fixture contains one snapshot
with no prices and one short scrape; if `int_snapshot_completeness` stops
flagging either, the build fails here.

The forecast marts get the same treatment. The fixture publishes two forecast
vintages, one old enough to be scoreable and one still in the future, so both
branches of mart_forecast_vs_actual run, and three backtest models with known
error, so the leaderboard in mart_forecast_accuracy has an order to assert on.

Usage:
    python ci/assert_quality.py --db data/ci.duckdb
"""

from __future__ import annotations

import argparse
import sys

import duckdb

EXPECT_INCOMPLETE = "2026-03-25"    # the short scrape
EXPECT_NO_RATES = "2025-12-22"      # the snapshot with no prices
EXPECT_CHAMPION = "seasonal_naive_drift"   # lowest seeded error of the three
EXPECT_BASELINE = "seasonal_naive"        # the model skill is measured against

failures: list[str] = []
checks = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global checks
    checks += 1
    print(("  PASS  " if ok else "  FAIL  ") + label + (f"   {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/ci.duckdb")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    one = lambda q: con.execute(q).fetchone()[0]
    col = lambda q: [r[0] for r in con.execute(q).fetchall()]

    print("\nsnapshot quality")
    bad = col("select snapshot_date::varchar from int_snapshot_completeness "
              "where not is_complete order by 1")
    check("the short scrape is flagged incomplete",
          bad == [EXPECT_INCOMPLETE], f"flagged: {bad or 'nothing'}")

    norate = col("select snapshot_date::varchar from int_snapshot_completeness "
                 "where not has_usable_rates order by 1")
    check("the priceless snapshot is flagged unusable for rates",
          norate == [EXPECT_NO_RATES], f"flagged: {norate or 'nothing'}")

    check("completeness and rate-usability stay independent",
          bad != norate, "a single flag would collapse two different failures")

    check("no snapshot verdict is null",
          one("select count(*) from int_snapshot_completeness "
              "where is_complete is null or has_usable_rates is null") == 0)

    print("\nfact table")
    rows = one("select count(*) from fct_listing_month")
    check("fct_listing_month is populated", rows > 0, f"{rows:,} rows")

    check("the priceless snapshot never supplies a rate",
          one(f"select count(*) from fct_listing_month "
              f"where rate_sourced_from = date '{EXPECT_NO_RATES}'") == 0)

    check("has_rate agrees with adr",
          one("select count(*) from fct_listing_month "
              "where has_rate <> (adr is not null)") == 0)

    over = one("select count(*) from fct_listing_month where estimated_occupancy > 0.7001")
    check("occupancy respects the cap", over == 0, f"{over} rows over")

    check("the trailing partial month is flagged",
          one("select count(*) from fct_listing_month where is_partial_month") > 0)

    print("\nmarts")
    for t in ("dim_listings", "mart_neighbourhood_monthly",
              "mart_neighbourhood_demand_monthly"):
        n = one(f"select count(*) from {t}")
        check(f"{t} is populated", n > 0, f"{n:,} rows")

    check("no partial month reaches the marts",
          one("select count(*) from mart_neighbourhood_demand_monthly m "
              "join (select distinct month_start from fct_listing_month "
              "      where is_partial_month) p on p.month_start = m.month_start") == 0)

    print("\nforecast inputs")
    vintages = one("select count(distinct trained_through) from forecast_weekly")
    check("forecast_weekly carries more than one vintage", vintages > 1,
          f"{vintages} vintages, so a refresh adds rather than overwrites")

    check(f"the backtest includes {EXPECT_BASELINE}, the skill baseline",
          one(f"select count(*) from forecast_backtest "
              f"where model = '{EXPECT_BASELINE}'") > 0)

    print("\nforecast marts")
    board = con.execute(
        "select model, rank_by_mae, skill_vs_baseline_pct "
        "from mart_forecast_accuracy order by rank_by_mae"
    ).fetchall()
    check("mart_forecast_accuracy is populated", len(board) > 0, f"{len(board)} models")

    check("every model is ranked once",
          sorted(r[1] for r in board) == list(range(1, len(board) + 1)))

    check(f"{EXPECT_CHAMPION} ranks first",
          bool(board) and board[0][0] == EXPECT_CHAMPION,
          f"first: {board[0][0] if board else 'nothing'}")

    check("the leaderboard keeps the models that lost",
          any(r[2] < 0 for r in board),
          "a board with no negative skill is not a comparison")

    scored = one("select count(*) from mart_forecast_vs_actual where abs_error is not null")
    check("mart_forecast_vs_actual scores the observed vintage", scored > 0,
          f"{scored} scored rows")

    check("no unobserved week is scored",
          one("select count(*) from mart_forecast_vs_actual "
              "where abs_error is not null and not week_fully_observed") == 0,
          "scoring a part-scraped week invents an error")

    check("every observed week with actuals is scored",
          one("select count(*) from mart_forecast_vs_actual "
              "where week_fully_observed and actual_reviews is not null "
              "and abs_error is null") == 0)

    bands = col("select distinct inside_80 from mart_forecast_vs_actual "
                "where inside_80 is not null order by 1")
    check("the 80% band is sometimes hit and sometimes missed",
          bands == [False, True],
          "a band that always contains the actual is not an interval")

    con.close()

    print(f"\n{checks - len(failures)}/{checks} checks passed")
    if failures:
        print("\nfailed:")
        for f in failures:
            print("  - " + f)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
