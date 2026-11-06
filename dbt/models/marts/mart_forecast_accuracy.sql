-- Model leaderboard from the rolling-origin backtest.
--
-- Every candidate is kept, not just the winner. A champion with nothing to
-- compare against is an assertion, and the two models that lost here are the
-- evidence for the two design decisions that matter: fourier_ridge_trend
-- losing to a trend-free baseline is what rules out fitting a trend through
-- the survivorship ramp, and seasonal_window_drift losing is what rules out
-- blurring the year-ago reference to absorb festival timing drift.

with scored as (
    select model, origin_week, horizon, abs_error, pct_error
    from {{ source('forecast', 'forecast_backtest') }}
    where forecast is not null
      and actual > 0
),

overall as (
    select
        model,
        count(*)                    as predictions,
        count(distinct origin_week) as folds,
        max(horizon)                as max_horizon,
        round(avg(abs_error), 2)    as mae,
        round(avg(pct_error), 2)    as mape
    from scored
    group by 1
),

baseline as (
    select coalesce(
        (select mae from overall where model = 'seasonal_naive'),
        (select max(mae) from overall)
    ) as baseline_mae
)

select
    o.model,
    o.folds,
    o.predictions,
    o.max_horizon,
    o.mae,
    o.mape,
    round((1 - o.mae / b.baseline_mae) * 100, 1) as skill_vs_baseline_pct,
    row_number() over (order by o.mae)           as rank_by_mae
from overall o
cross join baseline b
