"""Look at the weekly demand series before fitting anything to it.

Two known distortions to locate, not assume:
  Review lag    - a stay is reviewed days or weeks after checkout, so weeks
                  close to the scrape date are undercounted and taper.
  Survivorship  - attributes come from snapshots, so the further back the
                  series runs the more it reflects only listings that
                  survived into the snapshot window.
"""
from __future__ import annotations

import pathlib
import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = pathlib.Path(__file__).resolve().parents[1]
con = duckdb.connect(str(ROOT / "data" / "warehouse.duckdb"), read_only=True)

weekly = con.execute("""
    select date_trunc('week', review_date)::date as week_start,
           count(*)                   as reviews,
           count(distinct listing_id) as listings
    from stg_reviews
    group by 1 order by 1
""").df()
weekly["week_start"] = pd.to_datetime(weekly["week_start"])

last_snapshot = con.execute(
    "select max(snapshot_date) from stg_listings").fetchone()[0]

print(f"last snapshot: {last_snapshot}")
print(f"weeks: {len(weekly)}  from {weekly.week_start.min().date()} "
      f"to {weekly.week_start.max().date()}")

print("\n--- reviews per year (the survivorship ramp) ---")
by_year = weekly.assign(year=weekly.week_start.dt.year).groupby("year")["reviews"].sum()
for y, v in by_year.items():
    print(f"{y}  {v:>9,}")

print("\n--- last 12 weeks (look for the review-lag taper) ---")
for _, r in weekly.tail(12).iterrows():
    print(f"{r.week_start.date()}  {r.reviews:>6,}  listings {r.listings:>6,}")

print("\n--- busiest week in each year since 2023 ---")
w = weekly[weekly.week_start >= "2023-01-01"].copy()
w["year"] = w.week_start.dt.year
for y, grp in w.groupby("year"):
    top = grp.nlargest(3, "reviews")
    picks = ", ".join(f"{r.week_start.date()} ({r.reviews:,})"
                      for _, r in top.iterrows())
    print(f"{y}: {picks}")

print("\n--- does the seasonal shape repeat? monthly totals by year ---")
m = weekly.assign(year=weekly.week_start.dt.year, month=weekly.week_start.dt.month)
pivot = m[m.year >= 2024].pivot_table(
    index="month", columns="year", values="reviews", aggfunc="sum")
print(pivot.to_string(float_format=lambda x: f"{x:,.0f}"))

plot = weekly[weekly.week_start >= "2022-01-01"]
fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(plot.week_start, plot.reviews, lw=0.9, alpha=0.5, label="weekly")
ax.plot(plot.week_start, plot.reviews.rolling(4).mean(), lw=2,
        label="4-week mean")
ax.set_title("Austin short-term rental demand, weekly reviews as a stay proxy")
ax.set_ylabel("reviews")
ax.legend(frameon=False)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
out = ROOT / "analysis" / "demand_weekly.png"
fig.savefig(out, dpi=140)
print(f"\nchart: {out}")
