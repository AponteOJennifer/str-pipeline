-- Is each snapshot trustworthy, and trustworthy for what?
--
-- Two different failure modes, deliberately kept separate:
--
--   is_complete       - does this snapshot represent the whole market?
--                       Fails when the scrape came back short. Matters for
--                       anything that counts listings or sizes a market.
--
--   has_usable_rates  - does this snapshot carry prices?
--                       Inside Airbnb has published snapshots with a full
--                       listing set and an empty price column. Such a
--                       snapshot is fine for counting and useless for rate.
--
-- Conflating the two cost real data: the December 2025 Austin snapshot has
-- 11,251 listings and zero prices, and an earlier version of this model let
-- it anchor the rate lookup, which silently emptied the fact table.
--
-- Completeness uses two independent signals:
--   1. Size       - distance from the market median listing count.
--   2. Continuity - listings present in BOTH neighbouring snapshots but
--                   absent here. A listing cannot leave the market and come
--                   back; if it does, the scrape missed it. Judged as a
--                   share, since a few dozen is churn and a few thousand
--                   is a failure.
--
-- The continuity test needs a snapshot on each side, so the first and last
-- snapshots are judged on size alone rather than returning NULL. NULL here
-- is worse than a wrong answer: `where is_complete` drops it silently.

with snapshot_listings as (
    select snapshot_date, listing_id, nightly_price
    from {{ ref('stg_listings') }}
),

counts as (
    select
        snapshot_date,
        count(distinct listing_id) as listing_count,
        count(nightly_price)       as listings_with_price
    from snapshot_listings
    group by 1
),

neighbours as (
    select
        snapshot_date, listing_count, listings_with_price,
        lag(snapshot_date)  over (order by snapshot_date) as prev_snapshot,
        lead(snapshot_date) over (order by snapshot_date) as next_snapshot
    from counts
),

-- Left-anti: in the snapshot before and the one after, absent from this one.
gap_check as (
    select n.snapshot_date, count(*) as listings_missing_from_middle
    from neighbours n
    join snapshot_listings p
      on p.snapshot_date = n.prev_snapshot
    join snapshot_listings x
      on x.snapshot_date = n.next_snapshot
     and x.listing_id    = p.listing_id
    left join snapshot_listings c
      on c.snapshot_date = n.snapshot_date
     and c.listing_id    = p.listing_id
    where c.listing_id is null
    group by 1
),

market_median as (
    select median(listing_count) as median_listings from counts
)

select
    n.snapshot_date,
    n.listing_count,
    n.listings_with_price,
    m.median_listings,

    abs(n.listing_count - m.median_listings) / m.median_listings
        as deviation_from_median,

    n.listings_with_price::double / n.listing_count
        as price_coverage,

    coalesce(g.listings_missing_from_middle, 0)
        as listings_missing_from_middle,

    n.prev_snapshot is null or n.next_snapshot is null
        as is_edge_snapshot,

    n.listings_with_price::double / n.listing_count
        >= {{ var('min_price_coverage') }}
        as has_usable_rates,

    (
        abs(n.listing_count - m.median_listings) / m.median_listings
            <= {{ var('completeness_tolerance') }}
    )
    and coalesce(g.listings_missing_from_middle, 0)::double / n.listing_count
            <= {{ var('continuity_tolerance') }}
        as is_complete

from neighbours n
cross join market_median m
left join gap_check g on g.snapshot_date = n.snapshot_date
