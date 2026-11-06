-- Each published forecast against what actually happened.
--
-- This is the table that makes the forecast accountable over time. Because
-- forecast_weekly keys on trained_through, a week forecast from several
-- vintages appears once per vintage, so accuracy can be tracked by how far
-- ahead the call was made.
--
-- A week is only scored once the data fully covers it. The same coverage rule
-- the training set uses applies here: comparing a forecast against a partly
-- scraped week would manufacture a large error that says nothing about the
-- model.

with actuals as (
    select
        date_trunc('week', review_date)::date as week_start,
        count(*)                              as actual_reviews
    from {{ ref('stg_reviews') }}
    group by 1
),

coverage as (
    select max(snapshot_date) as last_snapshot
    from {{ ref('stg_listings') }}
),

published as (
    select * from {{ source('forecast', 'forecast_weekly') }}
),

joined as (
    select
        p.model,
        p.trained_through,
        p.target_week,
        p.horizon,
        p.forecast,
        p.lo80, p.hi80, p.lo95, p.hi95,
        a.actual_reviews,
        p.target_week + interval 6 day <= c.last_snapshot as week_fully_observed
    from published p
    cross join coverage c
    left join actuals a on a.week_start = p.target_week
)

select
    *,
    case when week_fully_observed and actual_reviews is not null
         then abs(actual_reviews - forecast) end                   as abs_error,
    case when week_fully_observed and coalesce(actual_reviews, 0) > 0
         then round(abs(actual_reviews - forecast)
                    / actual_reviews * 100, 2) end                 as pct_error,
    case when week_fully_observed and actual_reviews is not null
         then actual_reviews between lo80 and hi80 end             as inside_80,
    case when week_fully_observed and actual_reviews is not null
         then actual_reviews between lo95 and hi95 end             as inside_95
from joined
