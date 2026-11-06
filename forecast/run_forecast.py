"""Fit the champion model, forecast forward, and write results to the warehouse.

Run from the project root:

    python -m forecast.run_forecast
    python -m forecast.run_forecast --horizon 12 --model seasonal_naive_drift

Two tables are written, and both are keyed so a rerun replaces its own output
rather than duplicating it, the same delete-then-insert contract the ingest DAG
uses:

    forecast_weekly    one row per (model, trained_through, target_week)
    forecast_backtest  one row per (model, origin_week, target_week)

`trained_through` is part of the key on purpose. Rerunning after a new snapshot
lands adds a new vintage instead of overwriting the old one, so the accuracy of
past forecasts stays auditable against what actually happened. A forecast table
that only holds the current vintage can never be scored.

Prediction intervals are empirical, taken from the ratio of actual to forecast
across every backtest fold, and pooled across horizons. Pooling is justified by
the measurement rather than by convenience: the champion's error does not grow
with horizon, because it anchors on the same week last year rather than on
recent momentum. The diagnostic that checks this prints with the run, so if a
future refresh changes that, it shows up.
"""

from __future__ import annotations

import argparse
import datetime as dt

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .backtest import rolling_origin, summarise
from .models import default_models
from .series import ROOT, WAREHOUSE, WEEK, add_features, load_weekly

CHAMPION = "seasonal_naive_drift"


def pick(models: list, name: str):
    for m in models:
        if m.name == name:
            return m
    raise SystemExit(f"no model named {name}; have {[m.name for m in models]}")


def interval_ratios(results: pd.DataFrame, model: str) -> dict[str, float]:
    """Empirical actual/forecast quantiles, pooled across horizons."""
    d = results.loc[results["model"] == model].dropna(subset=["forecast"])
    d = d.loc[d["forecast"] > 0]
    ratios = (d["actual"] / d["forecast"]).to_numpy(dtype=float)

    # Does error grow with distance ahead? If it does, pooling is wrong.
    per_h = d.assign(ratio=np.abs(np.log(d["actual"] / d["forecast"]))) \
             .groupby("horizon")["ratio"].mean()
    slope = np.polyfit(per_h.index.to_numpy(dtype=float), per_h.to_numpy(), 1)[0]

    return {
        "lo80": float(np.quantile(ratios, 0.10)),
        "hi80": float(np.quantile(ratios, 0.90)),
        "lo95": float(np.quantile(ratios, 0.025)),
        "hi95": float(np.quantile(ratios, 0.975)),
        "n": int(len(ratios)),
        "horizon_slope": float(slope),
    }


def write_table(con, table: str, df: pd.DataFrame, keys: list[str]) -> int:
    """Delete-then-insert on the given keys, so a rerun cannot duplicate rows."""
    con.register("incoming", df)
    con.execute(f"create table if not exists {table} as select * from incoming limit 0")

    columns = {r[0] for r in con.execute(f"describe {table}").fetchall()}
    if set(df.columns) - columns:
        raise SystemExit(
            f"{table} is missing columns {sorted(set(df.columns) - columns)}. "
            f"The table predates this version of the script; drop it and rerun:\n"
            f"  python -c \"import duckdb; duckdb.connect('data/warehouse.duckdb')"
            f".execute('drop table {table}')\""
        )

    predicate = " and ".join(f"{table}.{k} = i.{k}" for k in keys)
    con.execute(
        f"delete from {table} where exists "
        f"(select 1 from incoming i where {predicate})"
    )
    con.execute(f"insert into {table} by name select * from incoming")
    return len(df)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2023-01-01")
    ap.add_argument("--horizon", type=int, default=12)
    ap.add_argument("--model", default=CHAMPION)
    ap.add_argument("--origins", type=int, default=12)
    ap.add_argument("--step", type=int, default=4)
    ap.add_argument("--min-train", type=int, default=78)
    ap.add_argument("--warehouse", default=str(WAREHOUSE))
    args = ap.parse_args()

    raw, last_snapshot = load_weekly(start=args.start, warehouse=args.warehouse)
    df = add_features(raw)
    trained_through = df["week_start"].max()

    print(f"trained through {trained_through.date()}  "
          f"({len(df)} weeks, snapshot {last_snapshot.date()})")

    # Backtest every candidate, so the accuracy table records the comparison
    # and not just the winner. A champion with nothing to compare against is
    # an assertion.
    models = default_models()
    results = rolling_origin(
        df, models, horizon=args.horizon, n_origins=args.origins,
        step=args.step, min_train=args.min_train,
    )
    summary = summarise(results)
    print("\n=== backtest ===")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))

    best_measured = summary.iloc[0]["model"]
    if best_measured != args.model:
        print(f"\nNOTE: {best_measured} measured better than the configured "
              f"champion {args.model}. Shipping {args.model} as asked; "
              f"rerun with --model {best_measured} to switch.")

    band = interval_ratios(results, args.model)
    print(f"\nintervals from {band['n']} backtest predictions: "
          f"80% [{band['lo80']:.3f}, {band['hi80']:.3f}]  "
          f"95% [{band['lo95']:.3f}, {band['hi95']:.3f}]")
    print(f"error growth per week of horizon: {band['horizon_slope']:+.5f} "
          f"log points ({'flat, pooling holds' if abs(band['horizon_slope']) < 0.004 else 'NOT flat, intervals should widen with horizon'})")

    champion = pick(models, args.model).fit(df)
    point = champion.predict(df, args.horizon)

    forecast = pd.DataFrame({
        "model": args.model,
        "trained_through": trained_through.date(),
        "target_week": [w.date() for w in point.index],
        "horizon": range(1, len(point) + 1),
        "forecast": point.to_numpy(dtype=float).round(1),
        "lo80": (point.to_numpy() * band["lo80"]).round(1),
        "hi80": (point.to_numpy() * band["hi80"]).round(1),
        "lo95": (point.to_numpy() * band["lo95"]).round(1),
        "hi95": (point.to_numpy() * band["hi95"]).round(1),
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    })

    print(f"\n=== forecast, {args.horizon} weeks from {trained_through.date()} ===")
    for _, r in forecast.iterrows():
        print(f"{r.target_week}  {r.forecast:>7,.0f}   "
              f"80% [{r.lo80:>7,.0f}, {r.hi80:>7,.0f}]")

    backtest_out = results.assign(
        trained_through=trained_through.date(),
        generated_at=dt.datetime.now().isoformat(timespec="seconds"),
    )
    for col in ("origin_week", "target_week"):
        backtest_out[col] = pd.to_datetime(backtest_out[col]).dt.date

    con = duckdb.connect(args.warehouse)
    try:
        n1 = write_table(con, "forecast_weekly", forecast,
                         ["model", "trained_through", "target_week"])
        n2 = write_table(con, "forecast_backtest", backtest_out,
                         ["model", "origin_week", "target_week"])
    finally:
        con.close()
    print(f"\nwrote {n1} rows to forecast_weekly, {n2} to forecast_backtest")

    out = ROOT / "analysis"
    out.mkdir(exist_ok=True)
    recent = df.loc[df["week_start"] >= trained_through - WEEK * 104]
    fut = pd.to_datetime(forecast["target_week"])

    fig, ax = plt.subplots(figsize=(13, 5.5))
    ax.plot(recent["week_start"], recent["reviews"], lw=1.4,
            color="#2b2b2b", label="actual")
    ax.plot(fut, forecast["forecast"], lw=2, color="#c2410c", label="forecast")
    ax.fill_between(fut, forecast["lo95"], forecast["hi95"],
                    color="#c2410c", alpha=0.12, lw=0, label="95%")
    ax.fill_between(fut, forecast["lo80"], forecast["hi80"],
                    color="#c2410c", alpha=0.22, lw=0, label="80%")
    ax.axvline(trained_through, color="#999", ls="--", lw=1)
    ax.set_ylabel("weekly reviews (stay proxy)")
    ax.set_title(f"Austin demand forecast, {args.model}, "
                 f"{band['n']}-fold backtest MAPE "
                 f"{float(summary.loc[summary['model'] == args.model, 'mape'].iloc[0]):.1f}%")
    ax.legend(frameon=False, ncol=4, fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out / "demand_forecast.png", dpi=140)
    print(f"wrote {out/'demand_forecast.png'}")


if __name__ == "__main__":
    main()
