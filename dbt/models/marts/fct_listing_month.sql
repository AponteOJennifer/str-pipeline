-- Listing by month: demand always, rate where one exists.
--
-- The two halves of this table have very different histories, and forcing
-- them to the same length throws away the longer one:
--
--   Demand - reviews stand in for stays, and the review history runs for
--            years. This is what a forecast can actually be built on.
--   Rate   - comes from snapshots, of which there are four, only three
--            carrying prices. Six months of rate, at best.
--
-- So the rate join is a LEFT as-of join and `has_rate` marks which rows
-- carry one. Requiring a rate would discard most of the demand history, and
-- an earlier version of this model did exactly that.
--
-- Demand method: the "San Francisco Model" scales review counts by an assumed
-- review rate and length of stay. Every assumption is a dbt var.
--
-- Rate method: a listing's price comes from the most recent price-bearing
-- snapshot at or before the month in question. Revenue management cares about
-- the rate in effect then, not today's rate.
--
-- Survivorship: listings delisted before the first snapshot have reviews but
-- no attributes, so they never enter this table. Demand before the snapshot
-- window is therefore biased toward listings that survived into it. The
-- demand_months var controls how far back to go; shorten it to trade history
-- for a cleaner panel.

with rate_snapshots as (
    select l.listing_id, l.snapshot_date, l.nightly_price
    from {{ ref('stg_listings') }} l
    join {{ ref('int_snapshot_completeness') }} c
      on c.snapshot_date = l.snapshot_date
    where c.has_usable_rates
      and l.nightly_price is not null
),

-- Reviews only exist up to the day of the most recent scrape, so the month
-- containing that date is captured in part. Left unflagged it reads as a
-- demand collapse, which is the same mistake as trusting a short snapshot.
snapshot_horizon as (
    select max(snapshot_date) as last_snapshot
    from {{ ref('stg_listings') }}
),

demand_window as (
    select
        date_trunc('month', max(review_date))
            - interval {{ var('demand_months') - 1 }} month as first_month,
        date_trunc('month', max(review_date))               as last_month
    from {{ ref('stg_reviews') }}
),

monthly_reviews as (
    select
        r.listing_id,
        date_trunc('month', r.review_date) as month_start,
        count(*)                           as reviews_in_month
    from {{ ref('stg_reviews') }} r
    cross join demand_window w
    where date_trunc('month', r.review_date) between w.first_month and w.last_month
    group by 1, 2
),

with_rate as (
    select
        m.listing_id,
        m.month_start,
        m.reviews_in_month,
        s.snapshot_date as rate_sourced_from,
        s.nightly_price
    from monthly_reviews m
    asof left join rate_snapshots s
      on  s.listing_id    =  m.listing_id
      and s.snapshot_date <= m.month_start
),

joined as (
    select
        w.*,
        a.neighbourhood, a.room_type, a.accommodates, a.minimum_nights,
        a.first_seen_snapshot, a.last_seen_snapshot
    from with_rate w
    join {{ ref('int_listing_attributes') }} a on a.listing_id = w.listing_id
),

estimated as (
    select
        *,
        date_diff('day', month_start, month_start + interval 1 month)
            as days_in_month,
        greatest(coalesce(minimum_nights, 0), {{ var('default_length_of_stay') }})
            as assumed_length_of_stay,
        reviews_in_month / {{ var('review_rate') }}
            as estimated_stays
    from joined
    cross join snapshot_horizon h
)

select
    listing_id,
    month_start,
    rate_sourced_from,
    nightly_price is not null as has_rate,
    (month_start + interval 1 month - interval 1 day) > last_snapshot
        as is_partial_month,

    neighbourhood,
    room_type,
    accommodates,
    first_seen_snapshot,
    last_seen_snapshot,

    reviews_in_month,
    estimated_stays,
    assumed_length_of_stay,
    days_in_month,

    estimated_stays * assumed_length_of_stay as estimated_nights_booked,

    least(
        estimated_stays * assumed_length_of_stay / days_in_month,
        {{ var('max_occupancy') }}
    ) as estimated_occupancy,

    nightly_price as adr,

    -- RevPAR: revenue per available night, the hotel metric applied to a unit
    -- instead of a room. ADR here is the listed rate, not an achieved rate,
    -- so this is an upper bound on realised RevPAR.
    nightly_price * least(
        estimated_stays * assumed_length_of_stay / days_in_month,
        {{ var('max_occupancy') }}
    ) as revpar

from estimated
