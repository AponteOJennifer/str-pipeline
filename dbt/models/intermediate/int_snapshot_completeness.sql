-- Flags snapshots that look incomplete, so downstream models can exclude them.
--
-- Inside Airbnb scrapes can come back partial. The March 2026 Austin snapshot
-- returned 8,170 listings against roughly 11,200 in the surrounding snapshots.
--
-- Two independent signals are computed:
--
--   1. Count deviation from the median across all snapshots. Cheap, but a real
--      market shift would look the same.
--   2. Listings absent from this snapshot that are present in BOTH the snapshot
--      before and the snapshot after. Listings do not leave a market and return
--      a quarter later, so a large number here means the scrape missed them.
--      This is the signal that actually distinguishes a bad scrape from a real
--      change in supply.

with snapshots as (
    select distinct snapshot_date
    from {{ ref('stg_listings') }}
),

sequenced as (
    select
        snapshot_date,
        lag(snapshot_date)  over (order by snapshot_date) as prev_snapshot,
        lead(snapshot_date) over (order by snapshot_date) as next_snapshot
    from snapshots
),

counts as (
    select
        snapshot_date,
        count(distinct listing_id) as listing_count
    from {{ ref('stg_listings') }}
    group by 1
),

median_count as (
    select median(listing_count) as median_listings
    from counts
),

-- Listings in both neighbouring snapshots but missing from this one.
reappearing as (
    select
        s.snapshot_date,
        count(*) as listings_missing_from_middle
    from sequenced s
    join {{ ref('stg_listings') }} p
      on p.snapshot_date = s.prev_snapshot
    join {{ ref('stg_listings') }} n
      on n.snapshot_date = s.next_snapshot
     and n.listing_id    = p.listing_id
    left join {{ ref('stg_listings') }} c
      on c.snapshot_date = s.snapshot_date
     and c.listing_id    = p.listing_id
    where c.listing_id is null
    group by 1
)

select
    c.snapshot_date,
    c.listing_count,
    m.median_listings,
    round((c.listing_count - m.median_listings) / m.median_listings, 4)
        as deviation_from_median,
    coalesce(r.listings_missing_from_middle, 0)
        as listings_missing_from_middle,
    abs((c.listing_count - m.median_listings) / m.median_listings)
        <= {{ var('completeness_tolerance') }}
        as is_complete
from counts c
cross join median_count m
left join reappearing r on r.snapshot_date = c.snapshot_date
order by c.snapshot_date
