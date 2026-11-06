"""Rolling-origin backtest.

A single train/test split on 190 weeks of seasonal data measures one draw from
a noisy process and tells you very little. This refits every model at a series
of origins stepped through the recent past, forecasts the full horizon from
each, and scores by how far ahead the forecast was. Error at week 1 and error
at week 12 are different quantities and collapsing them hides which models
degrade.

Scoring is MAE and MAPE for readability, plus skill against SeasonalNaive on
exactly the same folds. Skill is the number that matters: it answers whether
the model earns its complexity, and for this series it is also the test of
whether a fitted trend is picking up real growth or the survivorship ramp.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def rolling_origin(
    df: pd.DataFrame,
    models: list,
    horizon: int = 12,
    n_origins: int = 12,
    step: int = 4,
    min_train: int = 104,
) -> pd.DataFrame:
    """Refit at several origins; return one row per model, origin and horizon."""
    n = len(df)
    last_origin = n - horizon
    if last_origin < min_train:
        raise ValueError(
            f"need at least {min_train + horizon} weeks to backtest "
            f"{n_origins} origins at horizon {horizon}; have {n}"
        )

    origins = [last_origin - i * step for i in range(n_origins)]
    origins = sorted(o for o in origins if o >= min_train)
    if not origins:
        raise ValueError("no origin leaves enough training history")

    rows = []
    for origin in origins:
        train = df.iloc[:origin]
        truth = df.iloc[origin:origin + horizon]
        for model in models:
            fitted = model.fit(train)
            pred = fitted.predict(train, horizon)
            for h, (week, actual) in enumerate(
                zip(truth["week_start"], truth["reviews"]), start=1
            ):
                yhat = float(pred.get(week, np.nan))
                rows.append({
                    "model": model.name,
                    "origin_week": train["week_start"].max(),
                    "target_week": week,
                    "horizon": h,
                    "actual": float(actual),
                    "forecast": yhat,
                    "abs_error": abs(actual - yhat),
                    "pct_error": abs(actual - yhat) / actual * 100 if actual else np.nan,
                })
    return pd.DataFrame(rows)


def summarise(results: pd.DataFrame, baseline: str = "seasonal_naive") -> pd.DataFrame:
    """One row per model: MAE, MAPE, and skill against the baseline."""
    agg = (
        results.groupby("model")
        .agg(mae=("abs_error", "mean"),
             mape=("pct_error", "mean"),
             folds=("origin_week", "nunique"),
             n=("abs_error", "size"))
        .reset_index()
    )
    if baseline in set(agg["model"]):
        base_mae = float(agg.loc[agg["model"] == baseline, "mae"].iloc[0])
        agg["skill_vs_baseline_pct"] = (1 - agg["mae"] / base_mae) * 100
    return agg.sort_values("mae").reset_index(drop=True)


def by_horizon(results: pd.DataFrame) -> pd.DataFrame:
    """MAPE by model and horizon, to see which models decay with distance."""
    return (
        results.pivot_table(index="horizon", columns="model",
                            values="pct_error", aggfunc="mean")
        .round(2)
    )
