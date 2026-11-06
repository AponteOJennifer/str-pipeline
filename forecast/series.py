"""The weekly demand series, cleaned and featurised.

Reviews stand in for stays, so weekly review counts are the demand signal. Two
properties of the source shape everything here:

Coverage. A review can only exist if it was written before the scrape, so the
week containing the snapshot date is partly observed. The rule is strict: a
week is kept only if all seven of its days fall on or before the last snapshot.
The exploratory pass found no taper beyond that single week, so no further
buffer is subtracted. Throwing away real weeks to guard against a lag that
isn't there costs accuracy.

Survivorship. Listing attributes come from snapshots, so a listing delisted
before the first snapshot never appears, and the further back the series runs
the more it under-counts. Reviews by year run from 31 in 2009 to 146,569 in
2025, and almost none of that is market growth. Annualising 2026 gives roughly
the same total as 2025, so the market is close to flat. This matters for one
reason: a model with a free trend term will read the backwards decay as
forwards growth. That is a hypothesis to test in the backtest, not something to
assume, which is why FourierRidge takes a `trend` switch and why the baselines
are trend-free.
"""

from __future__ import annotations

import pathlib

import duckdb
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
HERE = pathlib.Path(__file__).resolve().parent
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
EVENTS = HERE / "events.csv"

SEASON = 52          # weeks per year
WEEK = pd.Timedelta(days=7)
YEAR = pd.Timedelta(days=364)   # 52 whole weeks, keeps weeks aligned


def load_weekly(start: str = "2023-01-01", warehouse=None) -> tuple[pd.DataFrame, pd.Timestamp]:
    """Weekly reviews and active listings, trimmed to fully covered weeks."""
    con = duckdb.connect(str(warehouse or WAREHOUSE), read_only=True)
    try:
        df = con.execute(
            """
            select date_trunc('week', review_date)::date as week_start,
                   count(*)                              as reviews,
                   count(distinct listing_id)            as listings
            from stg_reviews
            group by 1
            order by 1
            """
        ).df()
        last_snapshot = con.execute(
            "select max(snapshot_date) from stg_listings"
        ).fetchone()[0]
    finally:
        con.close()

    df["week_start"] = pd.to_datetime(df["week_start"])
    last_snapshot = pd.Timestamp(last_snapshot)

    covered = df["week_start"] + pd.Timedelta(days=6) <= last_snapshot
    df = df.loc[covered & (df["week_start"] >= pd.Timestamp(start))]
    return df.reset_index(drop=True), last_snapshot


def load_events() -> pd.DataFrame:
    """One-off anomalies that recurring seasonality cannot represent.

    Recurring, date-anchored events (SXSW in March, ACL and the Grand Prix in
    October) are already carried by the Fourier terms, because they land in
    roughly the same week every year. What Fourier cannot represent is a
    one-time shock. April 2024 is the case in this data: 15,650 reviews against
    March's 10,772, inverting the SXSW pattern that holds in every other year,
    with the peak week of the entire year falling on 8 April. That is the total
    solar eclipse, which put Austin in the path of totality. Unflagged, it
    inflates the April seasonal index for every future April.
    """
    if not EVENTS.exists():
        return pd.DataFrame(columns=["name", "category", "start_date", "end_date"])
    ev = pd.read_csv(EVENTS, parse_dates=["start_date", "end_date"])
    return ev


def add_features(df: pd.DataFrame, harmonics: int = 5) -> pd.DataFrame:
    """Calendar and event features. No lagged target here; models add their own."""
    out = df.copy()

    # Position within the year as a fraction, so the Fourier basis is smooth
    # across the year boundary. day-of-year over 365.25 rather than ISO week
    # avoids the 53-week-year discontinuity.
    frac = out["week_start"].dt.dayofyear / 365.25
    for k in range(1, harmonics + 1):
        out[f"sin_{k}"] = np.sin(2 * np.pi * k * frac)
        out[f"cos_{k}"] = np.cos(2 * np.pi * k * frac)

    # Weeks since the start of the series. Only used by models that opt into a
    # trend; see the survivorship note in this module's docstring.
    out["trend"] = (out["week_start"] - out["week_start"].min()).dt.days / 7.0

    events = load_events()
    for category in sorted(events["category"].unique()) if len(events) else []:
        spans = events.loc[events["category"] == category]
        hit = np.zeros(len(out), dtype=float)
        for _, row in spans.iterrows():
            # A week is affected if it overlaps the event at all.
            overlaps = (out["week_start"] <= row["end_date"]) & (
                out["week_start"] + pd.Timedelta(days=6) >= row["start_date"]
            )
            hit = np.maximum(hit, overlaps.to_numpy(dtype=float))
        out[f"event_{category}"] = hit

    return out


def feature_columns(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c.startswith(("sin_", "cos_", "event_"))
    ]
