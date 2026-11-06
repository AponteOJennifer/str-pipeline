"""Build a small, deterministic warehouse for CI.

The real warehouse is 641 MB and gitignored, so continuous integration has no
data to build against. This writes a fixture that is a miniature of it: four
snapshots on the real dates, a stable listing universe, and reviews spread
across three years.

The fixture is seeded with the two defects found in the real Austin data, on
purpose:

  2025-12-22  every listing present, no prices at all
  2026-03-25  a quarter of the listings missing, then back the next snapshot

That makes CI a regression test on the data quality models rather than a
compile check. If a future change stops `int_snapshot_completeness` flagging
these, the build fails. See ci/assert_quality.py for the assertions.

The forecast tables are written too. dbt declares forecast_weekly and
forecast_backtest as sources because forecast/run_forecast.py produces them, so
a fixture holding only the raw ingest tables leaves those sources dangling:
their tests error and the two marts that read them are skipped. Writing them
makes the fixture a miniature of the warehouse after both steps have run, and
keeps all ten models under test.

Usage:
    python ci/build_fixture.py --out data/ci.duckdb
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import pathlib
import random

import duckdb

SNAPSHOTS = ["2025-12-22", "2026-03-25", "2026-06-22", "2026-09-17"]
CITY = "austin"
N_LISTINGS = 80
N_NEIGHBOURHOODS = 8          # 10 listings each, clears the marts' >= 5 filter

# Which listings each snapshot contains, and whether it carries prices.
SNAPSHOT_PLAN = {
    "2025-12-22": {"listings": N_LISTINGS, "prices": False},   # defect 1
    "2026-03-25": {"listings": 56,         "prices": True},    # defect 2
    "2026-06-22": {"listings": N_LISTINGS, "prices": True},
    "2026-09-17": {"listings": 78,         "prices": True},
}

ROOM_TYPES = ["Entire home/apt", "Private room", "Shared room", "Hotel room"]

LISTING_COLS = [
    "id", "host_id", "host_since", "host_is_superhost",
    "neighbourhood_cleansed", "latitude", "longitude",
    "property_type", "room_type", "accommodates", "bedrooms", "beds",
    "price", "minimum_nights", "maximum_nights",
    "availability_30", "availability_60", "availability_90", "availability_365",
    "number_of_reviews", "number_of_reviews_ltm", "first_review", "last_review",
    "review_scores_rating", "reviews_per_month",
    "city", "snapshot_date",
]

REVIEW_COLS = ["id", "listing_id", "date", "reviewer_id", "reviewer_name",
               "comments", "city", "snapshot_date"]

# Error by model, so the leaderboard in mart_forecast_accuracy comes out in a
# stable order and mirrors what the real backtest found: the drift variant wins,
# a fitted trend loses to a trend-free baseline.
MODEL_ERROR = {
    "seasonal_naive_drift": 0.03,
    "seasonal_naive": 0.08,
    "fourier_ridge_trend": 0.14,
}
CHAMPION = "seasonal_naive_drift"
HORIZON = 8
GENERATED_AT = "2026-09-18T06:00:00"      # fixed, so the fixture is reproducible


def listings_rows() -> list[tuple]:
    rng = random.Random(11)
    rows = []
    for snap in SNAPSHOTS:
        plan = SNAPSHOT_PLAN[snap]
        for i in range(1, plan["listings"] + 1):
            price = None if not plan["prices"] else 90.0 + (i % 23) * 11.0
            rows.append((
                i,                                        # id
                100000 + i,                               # host_id
                "2019-04-02",                             # host_since
                "t" if i % 3 == 0 else "f",               # host_is_superhost
                f"787{30 + (i % N_NEIGHBOURHOODS)}",      # neighbourhood_cleansed
                30.26 + (i % 20) * 0.004,                 # latitude
                -97.74 - (i % 20) * 0.004,                # longitude
                "Entire residential home",                # property_type
                ROOM_TYPES[i % len(ROOM_TYPES)],          # room_type
                2 + (i % 6),                              # accommodates
                1 + (i % 3),                              # bedrooms
                1 + (i % 4),                              # beds
                price,                                    # price
                1 + (i % 4),                              # minimum_nights
                365,                                      # maximum_nights
                rng.randint(0, 30), rng.randint(0, 60),
                rng.randint(0, 90), rng.randint(0, 365),
                rng.randint(5, 400),                      # number_of_reviews
                rng.randint(0, 60),                       # number_of_reviews_ltm
                "2021-06-01", "2026-09-01",               # first_review, last_review
                4.2 + (i % 8) * 0.1,                      # review_scores_rating
                round(0.4 + (i % 9) * 0.3, 2),            # reviews_per_month
                CITY, snap,
            ))
    return rows


def reviews_rows() -> list[tuple]:
    """Reviews for every listing across three years, up to the last scrape."""
    rng = random.Random(29)
    rows, rid = [], 0
    months = [(y, m) for y in (2024, 2025, 2026) for m in range(1, 13)]
    months = [(y, m) for (y, m) in months if not (y == 2026 and m > 9)]
    for (y, m) in months:
        # March and October run hot, the way SXSW and ACL do in the real data.
        busy = 2 if m in (3, 10) else 0
        # Reviews land on any day of the real month, so weekly totals do not
        # dip at month boundaries. September 2026 stops at the 12th: the last
        # snapshot is the 17th, so the final week is only part scraped. Its
        # volume is scaled to the shorter window, otherwise a full month of
        # reviews crammed into 12 days reads as a demand spike.
        days = calendar.monthrange(y, m)[1]
        last_day = 12 if (y == 2026 and m == 9) else days
        for listing in range(1, N_LISTINGS + 1):
            n = rng.randint(1, 3) + busy
            if last_day < days:
                n = round(n * last_day / days)
            for _ in range(n):
                rid += 1
                day = rng.randint(1, last_day)
                rows.append((
                    rid, listing, f"{y}-{m:02d}-{day:02d}",
                    500000 + rid, "Guest", "Lovely stay.",
                    CITY, SNAPSHOTS[-1],
                ))
    return rows


def monday(d: dt.date) -> dt.date:
    return d - dt.timedelta(days=d.weekday())


def weekly_series(review_rows: list[tuple]) -> list[tuple[dt.date, int]]:
    """Weekly review counts, the aggregation stg_reviews feeds the marts."""
    counts: dict[dt.date, int] = {}
    for r in review_rows:
        w = monday(dt.date.fromisoformat(r[2]))
        counts[w] = counts.get(w, 0) + 1
    return sorted(counts.items())


def forecast_rows(review_rows: list[tuple], last_snapshot: dt.date):
    """Two published vintages and a small rolling-origin backtest.

    The numbers come from the fixture's own reviews rather than being invented,
    so forecast_backtest.actual agrees with what stg_reviews reports for the
    same week and the marts compute something coherent.

    Two vintages on purpose. One trained far enough back that its target weeks
    have since been observed, so mart_forecast_vs_actual scores it; one current,
    whose target weeks are still in the future and cannot be scored. That is the
    reason trained_through is part of the key, and the unobserved branch of that
    mart would otherwise never run in CI.
    """
    observed = [(w, n) for (w, n) in weekly_series(review_rows)
                if w + dt.timedelta(days=6) <= last_snapshot]
    actual = dict(observed)
    weeks = [w for (w, _) in observed]
    if len(weeks) < 60:
        raise SystemExit(f"only {len(weeks)} fully observed weeks, need 60+")
    mean_n = sum(actual.values()) / len(weeks)

    backtest = []
    for origin in (weeks[-(HORIZON + 12)], weeks[-(HORIZON + 4)]):
        start = weeks.index(origin) + 1
        for name, base in MODEL_ERROR.items():
            for h, week in enumerate(weeks[start:start + HORIZON], start=1):
                a = float(actual[week])
                err = base * (1 + (h % 3) * 0.1)          # deterministic jitter
                yhat = round(a * (1 + (err if h % 2 == 0 else -err)), 1)
                backtest.append((
                    name, origin, week, h, a, yhat,
                    round(abs(a - yhat), 4), round(abs(a - yhat) / a * 100, 4),
                    weeks[-1], GENERATED_AT,
                ))

    published = []

    def band(f: float) -> tuple:
        return (round(f * 0.93, 1), round(f * 1.07, 1),
                round(f * 0.86, 1), round(f * 1.14, 1))

    # Vintage A: trained back far enough that every target week is observed.
    a_through = weeks[-(HORIZON + 2)]
    a_start = weeks.index(a_through) + 1
    for h, week in enumerate(weeks[a_start:a_start + HORIZON], start=1):
        a = float(actual[week])
        # One week deliberately outside the 80% band, so inside_80 is not
        # constant and a band collapsed to the point estimate would show.
        off = 0.12 if h == 3 else 0.03
        f = round(a * (1 + (off if h % 2 == 0 else -off)), 1)
        published.append((CHAMPION, a_through, week, h, f, *band(f), GENERATED_AT))

    # Vintage B: the current one, forecasting into weeks nobody has seen yet.
    b_through = weeks[-1]
    for h in range(1, HORIZON + 1):
        week = b_through + dt.timedelta(days=7 * h)
        f = round(float(actual.get(week - dt.timedelta(days=364), mean_n)), 1)
        published.append((CHAMPION, b_through, week, h, f, *band(f), GENERATED_AT))

    return published, backtest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/ci.duckdb")
    args = ap.parse_args()

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()

    con = duckdb.connect(str(out))
    try:
        lst, rev = listings_rows(), reviews_rows()
        con.execute(
            "create table raw_listings ("
            "id BIGINT, host_id BIGINT, host_since VARCHAR, host_is_superhost VARCHAR,"
            "neighbourhood_cleansed VARCHAR, latitude DOUBLE, longitude DOUBLE,"
            "property_type VARCHAR, room_type VARCHAR, accommodates BIGINT,"
            "bedrooms BIGINT, beds BIGINT, price DOUBLE, minimum_nights BIGINT,"
            "maximum_nights BIGINT, availability_30 BIGINT, availability_60 BIGINT,"
            "availability_90 BIGINT, availability_365 BIGINT, number_of_reviews BIGINT,"
            "number_of_reviews_ltm BIGINT, first_review VARCHAR, last_review VARCHAR,"
            "review_scores_rating DOUBLE, reviews_per_month DOUBLE,"
            "city VARCHAR, snapshot_date VARCHAR)"
        )
        con.executemany(
            f"insert into raw_listings values ({','.join('?' * len(LISTING_COLS))})", lst
        )
        con.execute(
            "create table raw_reviews ("
            "id BIGINT, listing_id BIGINT, date VARCHAR, reviewer_id BIGINT,"
            "reviewer_name VARCHAR, comments VARCHAR, city VARCHAR, snapshot_date VARCHAR)"
        )
        con.executemany(
            f"insert into raw_reviews values ({','.join('?' * len(REVIEW_COLS))})", rev
        )

        pub, bt = forecast_rows(rev, dt.date.fromisoformat(SNAPSHOTS[-1]))
        con.execute(
            "create table forecast_weekly ("
            "model VARCHAR, trained_through DATE, target_week DATE, horizon BIGINT,"
            "forecast DOUBLE, lo80 DOUBLE, hi80 DOUBLE, lo95 DOUBLE, hi95 DOUBLE,"
            "generated_at VARCHAR)"
        )
        con.executemany(
            f"insert into forecast_weekly values ({','.join('?' * 10)})", pub
        )
        con.execute(
            "create table forecast_backtest ("
            "model VARCHAR, origin_week DATE, target_week DATE, horizon BIGINT,"
            "actual DOUBLE, forecast DOUBLE, abs_error DOUBLE, pct_error DOUBLE,"
            "trained_through DATE, generated_at VARCHAR)"
        )
        con.executemany(
            f"insert into forecast_backtest values ({','.join('?' * 10)})", bt
        )

        print(f"{out}: {len(lst):,} listing-snapshots, {len(rev):,} reviews")
        for snap, n, px in con.execute(
            "select snapshot_date, count(*), count(price) from raw_listings "
            "group by 1 order by 1"
        ).fetchall():
            note = "  <- seeded defect: no prices" if px == 0 else (
                "  <- seeded defect: short scrape" if n < 70 else "")
            print(f"  {snap}  {n:>3} listings, {px:>3} priced{note}")
        print(f"  forecast_weekly   {len(pub):>3} rows, "
              f"{len({r[1] for r in pub})} vintages")
        print(f"  forecast_backtest {len(bt):>3} rows, "
              f"{len({r[0] for r in bt})} models")
    finally:
        con.close()


if __name__ == "__main__":
    main()
