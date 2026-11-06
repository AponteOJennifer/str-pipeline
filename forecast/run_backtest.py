"""Backtest every candidate model and report which one earns its keep.

Run from the project root:

    python -m forecast.run_backtest
    python -m forecast.run_backtest --start 2022-01-01 --horizon 12
"""

from __future__ import annotations

import argparse
import pathlib

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .backtest import by_horizon, rolling_origin, summarise
from .models import default_models
from .series import ROOT, add_features, load_weekly


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2023-01-01",
                    help="first week to train on; earlier weeks are more "
                         "distorted by survivorship")
    ap.add_argument("--horizon", type=int, default=12)
    ap.add_argument("--origins", type=int, default=12)
    ap.add_argument("--step", type=int, default=4)
    ap.add_argument("--min-train", type=int, default=78)
    args = ap.parse_args()

    raw, last_snapshot = load_weekly(start=args.start)
    df = add_features(raw)

    print(f"last snapshot      {last_snapshot.date()}")
    print(f"weeks for training {len(df)}  "
          f"({df.week_start.min().date()} to {df.week_start.max().date()})")
    print(f"weekly reviews     mean {df.reviews.mean():,.0f}  "
          f"min {df.reviews.min():,.0f}  max {df.reviews.max():,.0f}")
    flagged = int(df.filter(like="event_").to_numpy().sum())
    print(f"event weeks        {flagged}")

    results = rolling_origin(
        df, default_models(),
        horizon=args.horizon, n_origins=args.origins,
        step=args.step, min_train=args.min_train,
    )

    summary = summarise(results)
    print("\n=== backtest summary, averaged over all origins and horizons ===")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))

    print("\n=== MAPE by weeks ahead ===")
    print(by_horizon(results).to_string())

    origins = results["origin_week"].nunique()
    span = (results["origin_week"].min().date(), results["origin_week"].max().date())
    print(f"\n{origins} origins from {span[0]} to {span[1]}, "
          f"horizon {args.horizon} weeks, {len(results):,} scored predictions")

    best = summary.iloc[0]
    print(f"\nbest by MAE: {best['model']}  "
          f"MAE {best['mae']:,.0f}  MAPE {best['mape']:.2f}%  "
          f"skill vs seasonal naive {best.get('skill_vs_baseline_pct', 0):+.1f}%")

    out = ROOT / "analysis"
    out.mkdir(exist_ok=True)

    # Error growth with distance ahead, which is what a planner actually needs
    # to know: how far out the forecast stays usable.
    fig, ax = plt.subplots(figsize=(10, 5))
    for model, grp in results.groupby("model"):
        prof = grp.groupby("horizon")["pct_error"].mean()
        ax.plot(prof.index, prof.values, marker="o", ms=4, label=model)
    ax.set_xlabel("weeks ahead")
    ax.set_ylabel("MAPE (%)")
    ax.set_title("Forecast error by horizon, rolling-origin backtest")
    ax.legend(frameon=False, fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out / "backtest_by_horizon.png", dpi=140)

    results.to_csv(out / "backtest_raw.csv", index=False)
    summary.to_csv(out / "backtest_summary.csv", index=False)
    print(f"\nwrote {out/'backtest_by_horizon.png'}")
    print(f"wrote {out/'backtest_summary.csv'}")


if __name__ == "__main__":
    main()
