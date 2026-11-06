"""Forecasting models, from a trend-free baseline up.

Every model takes a training frame and produces a forecast for a given number
of weeks beyond it. The interface is deliberately narrow so the backtest can
treat them interchangeably.

The baselines are not filler. The survivorship ramp described in series.py
means a model that fits a trend can look good in-sample while forecasting
growth that is an artifact of the source. SeasonalNaive cannot do that, so it
is the number every other model has to beat. A model that cannot beat it is
not worth shipping, however sophisticated it is.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge

from .series import SEASON, WEEK, YEAR, feature_columns


def future_index(train: pd.DataFrame, horizon: int) -> pd.DatetimeIndex:
    last = train["week_start"].max()
    return pd.DatetimeIndex([last + WEEK * (h + 1) for h in range(horizon)])


class SeasonalNaive:
    """The same week one year ago. No trend, no parameters, hard to beat."""

    name = "seasonal_naive"

    def fit(self, train: pd.DataFrame):
        self.history = train.set_index("week_start")["reviews"].astype(float)
        return self

    def _lookup(self, week: pd.Timestamp) -> float:
        # Weeks are Monday-aligned, so a 364-day step lands on a Monday too.
        # Fall back to the nearest available week if that exact one is missing.
        target = week - YEAR
        if target in self.history.index:
            return float(self.history.loc[target])
        if len(self.history) == 0:
            return np.nan
        pos = self.history.index.get_indexer([target], method="nearest")[0]
        return float(self.history.iloc[pos])

    def predict(self, train: pd.DataFrame, horizon: int) -> pd.Series:
        idx = future_index(train, horizon)
        return pd.Series([self._lookup(w) for w in idx], index=idx)


class SeasonalNaiveDrift(SeasonalNaive):
    """Last year's week, rescaled by how the recent level compares to a year ago.

    Keeps the seasonal shape from the long history but takes the level from the
    recent window, which is the part of the series survivorship does not
    distort. The ratio is clipped because on a short window it can blow up.
    """

    name = "seasonal_naive_drift"

    def __init__(self, lookback: int = 13, clip: tuple[float, float] = (0.6, 1.6)):
        self.lookback = lookback
        self.clip = clip

    def fit(self, train: pd.DataFrame):
        super().fit(train)
        recent = self.history.iloc[-self.lookback:]
        prior_weeks = [w - YEAR for w in recent.index]
        prior = self.history.reindex(prior_weeks).dropna()
        if len(prior) < max(4, self.lookback // 2) or prior.sum() <= 0:
            self.ratio = 1.0
        else:
            matched = self.history.reindex([w + YEAR for w in prior.index]).dropna()
            self.ratio = float(
                np.clip(matched.sum() / prior.sum(), *self.clip)
            )
        return self

    def predict(self, train: pd.DataFrame, horizon: int) -> pd.Series:
        return super().predict(train, horizon) * self.ratio


class FourierRidge:
    """Ridge on log reviews with a Fourier year, optional trend, event flags.

    Fitted on log1p so seasonality is multiplicative, which is how demand
    actually behaves: a festival adds a percentage, not a fixed count.

    `trend` exists to be tested rather than trusted. With it on, the model can
    fit the survivorship ramp and project it forward as growth. The backtest
    reports both so the comparison is explicit.
    """

    def __init__(self, trend: bool = False, alpha: float = 1.0, name: str | None = None):
        self.trend = trend
        self.alpha = alpha
        self.name = name or ("fourier_ridge_trend" if trend else "fourier_ridge")

    def _design(self, df: pd.DataFrame, trend_origin: pd.Timestamp) -> np.ndarray:
        cols = [df[c].to_numpy(dtype=float) for c in self.features]
        if self.trend:
            weeks = (df["week_start"] - trend_origin).dt.days.to_numpy() / 7.0
            cols.append(weeks)
        return np.column_stack(cols)

    def fit(self, train: pd.DataFrame):
        self.features = feature_columns(train)
        self.trend_origin = train["week_start"].min()
        X = self._design(train, self.trend_origin)
        y = np.log1p(train["reviews"].to_numpy(dtype=float))
        self.model = Ridge(alpha=self.alpha).fit(X, y)
        return self

    def predict(self, train: pd.DataFrame, horizon: int) -> pd.Series:
        from .series import add_features

        idx = future_index(train, horizon)
        future = add_features(pd.DataFrame({"week_start": idx}))
        # add_features can only build columns it has data for; align to training.
        for col in self.features:
            if col not in future:
                future[col] = 0.0
        X = self._design(future, self.trend_origin)
        return pd.Series(np.expm1(self.model.predict(X)), index=idx).clip(lower=0)


class GbmSeasonalLags:
    """Gradient boosting on calendar features plus year-ago lags.

    Only lags of 51 weeks and longer are used. That is a deliberate constraint:
    it means any horizon up to a year can be predicted directly from observed
    history, with no recursive feeding of predictions back in as inputs, which
    is where multi-step forecasts usually accumulate error.
    """

    name = "gbm_seasonal_lags"
    LAGS = (SEASON - 1, SEASON, SEASON + 1)

    def __init__(self, **kwargs):
        self.params = dict(
            max_iter=300, learning_rate=0.05, max_depth=3,
            min_samples_leaf=8, l2_regularization=1.0, random_state=0,
        )
        self.params.update(kwargs)

    def _with_lags(self, frame: pd.DataFrame, history: pd.Series) -> pd.DataFrame:
        out = frame.copy()
        for lag in self.LAGS:
            want = [w - WEEK * lag for w in out["week_start"]]
            out[f"lag_{lag}"] = history.reindex(want).to_numpy(dtype=float)
        return out

    def _cols(self) -> list[str]:
        return self.features + [f"lag_{lag}" for lag in self.LAGS]

    def fit(self, train: pd.DataFrame):
        self.features = feature_columns(train)
        self.history = train.set_index("week_start")["reviews"].astype(float)
        frame = self._with_lags(train, self.history)
        usable = frame.dropna(subset=[f"lag_{lag}" for lag in self.LAGS])
        if len(usable) < 30:
            # Not enough overlap to learn year-ago structure; defer.
            self.model = None
            self.fallback = SeasonalNaiveDrift().fit(train)
            return self
        self.model = HistGradientBoostingRegressor(**self.params).fit(
            usable[self._cols()].to_numpy(dtype=float),
            np.log1p(usable["reviews"].to_numpy(dtype=float)),
        )
        return self

    def predict(self, train: pd.DataFrame, horizon: int) -> pd.Series:
        from .series import add_features

        if self.model is None:
            return self.fallback.predict(train, horizon)
        idx = future_index(train, horizon)
        future = add_features(pd.DataFrame({"week_start": idx}))
        for col in self.features:
            if col not in future:
                future[col] = 0.0
        future = self._with_lags(future, self.history)
        X = future[self._cols()].to_numpy(dtype=float)
        return pd.Series(np.expm1(self.model.predict(X)), index=idx).clip(lower=0)





class SeasonalWindowDrift(SeasonalNaiveDrift):
    """Blend a window of weeks around the year-ago reference, then rescale.

    SeasonalNaiveDrift looks up one week, 364 days back, and that is where its
    largest remaining error comes from. Austin's festivals do not keep to a
    fixed calendar week: SXSW peaked the week of 10 March in 2025 and 16 March
    in 2026. A single-week lookup therefore misses twice, forecasting high on
    the week the festival left and low on the week it moved to.

    Averaging a weighted window across the year-ago neighbourhood trades peak
    sharpness for immunity to a one-week shift. Whether that trade is worth
    making is a question for the backtest, not an assumption: on a series with
    stable event timing this would be strictly worse.
    """

    name = "seasonal_window_drift"

    def __init__(self, weights=(0.25, 0.5, 0.25), **kwargs):
        super().__init__(**kwargs)
        self.weights = np.asarray(weights, dtype=float)
        self.weights = self.weights / self.weights.sum()
        self.offsets = range(-(len(self.weights) // 2), len(self.weights) // 2 + 1)

    def _lookup(self, week: pd.Timestamp) -> float:
        vals, wts = [], []
        for w, off in zip(self.weights, self.offsets):
            target = week - YEAR + WEEK * off
            if target in self.history.index:
                vals.append(float(self.history.loc[target]))
                wts.append(w)
        if not vals:
            return super()._lookup(week)
        return float(np.average(vals, weights=wts))


class GbmSeasonalRatio:
    """Gradient boosting on the year-over-year ratio rather than the level.

    Two problems are solved by the choice of target. Differencing at the
    seasonal lag removes the level, so the survivorship ramp cannot be mistaken
    for growth and no trend term is needed. And the shape features describe the
    year-ago neighbourhood relative to the year-ago week itself, which is how
    the model can recognise "the peak sat next to this week last year" and
    shift its correction accordingly.

    The ratio the model learns is the average over its training period. The
    current growth regime is unlikely to equal that average, so the forecast is
    re-centred by the difference between the recent year-over-year ratio and
    the training mean. That keeps the model's job to explaining calendar
    structure and leaves the level to a measurement of the recent past.
    """

    name = "gbm_seasonal_ratio"
    SHAPE_LAGS = (SEASON - 2, SEASON - 1, SEASON + 1, SEASON + 2)

    def __init__(self, lookback: int = 13, **kwargs):
        self.lookback = lookback
        self.params = dict(
            max_iter=250, learning_rate=0.05, max_depth=3,
            min_samples_leaf=10, l2_regularization=1.0, random_state=0,
        )
        self.params.update(kwargs)

    def _frame(self, frame: pd.DataFrame, history: pd.Series) -> pd.DataFrame:
        out = frame.copy()
        log_hist = np.log1p(history)
        anchor = log_hist.reindex(
            [w - WEEK * SEASON for w in out["week_start"]]).to_numpy(dtype=float)
        out["anchor"] = anchor
        for lag in self.SHAPE_LAGS:
            near = log_hist.reindex(
                [w - WEEK * lag for w in out["week_start"]]).to_numpy(dtype=float)
            out[f"shape_{lag}"] = near - anchor
        return out

    def _cols(self) -> list[str]:
        return self.features + [f"shape_{lag}" for lag in self.SHAPE_LAGS]

    def fit(self, train: pd.DataFrame):
        self.features = feature_columns(train)
        self.history = train.set_index("week_start")["reviews"].astype(float)
        frame = self._frame(train, self.history)
        frame["target"] = np.log1p(train["reviews"].to_numpy(dtype=float)) - frame["anchor"]
        usable = frame.dropna(subset=self._cols() + ["target"])

        if len(usable) < 30:
            self.model = None
            self.fallback = SeasonalWindowDrift().fit(train)
            return self

        self.train_mean_ratio = float(usable["target"].mean())
        self.model = HistGradientBoostingRegressor(**self.params).fit(
            usable[self._cols()].to_numpy(dtype=float),
            usable["target"].to_numpy(dtype=float),
        )

        # Re-centre on the recent growth regime rather than the training mean.
        recent = self.history.iloc[-self.lookback:]
        prior = self.history.reindex([w - YEAR for w in recent.index])
        mask = prior.notna().to_numpy()
        if mask.sum() >= max(4, self.lookback // 2):
            recent_ratio = float(
                np.log1p(recent.to_numpy()[mask]).mean()
                - np.log1p(prior.to_numpy()[mask]).mean()
            )
            self.recentre = float(np.clip(recent_ratio - self.train_mean_ratio, -0.5, 0.5))
        else:
            self.recentre = 0.0
        return self

    def predict(self, train: pd.DataFrame, horizon: int) -> pd.Series:
        from .series import add_features

        if self.model is None:
            return self.fallback.predict(train, horizon)
        idx = future_index(train, horizon)
        future = add_features(pd.DataFrame({"week_start": idx}))
        for col in self.features:
            if col not in future:
                future[col] = 0.0
        future = self._frame(future, self.history)
        X = future[self._cols()].to_numpy(dtype=float)
        pred_log = future["anchor"].to_numpy(dtype=float) + self.model.predict(X) + self.recentre
        return pd.Series(np.expm1(pred_log), index=idx).clip(lower=0)


def default_models() -> list:
    return [
        SeasonalNaive(),
        SeasonalNaiveDrift(),
        SeasonalWindowDrift(),
        FourierRidge(trend=False),
        FourierRidge(trend=True),
        GbmSeasonalLags(),
        GbmSeasonalRatio(),
    ]
