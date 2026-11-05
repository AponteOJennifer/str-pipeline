-- Current attributes per listing, taken from the most recent COMPLETE snapshot
-- in which the listing appears. Incomplete snapshots are excluded so a partial
-- scrape cannot silently become a listing's "current" state.

with complete_snapshots as (
    select snapshot_date
    from {{ ref('int_snapshot_completeness') }}
    where is_complete
),

ranked as (
    select
        l.*,
        row_number() over (
            partition by l.listing_id
            order by l.snapshot_date desc
        ) as rn
    from {{ ref('stg_listings') }} l
    join complete_snapshots c on c.snapshot_date = l.snapshot_date
)

select
    listing_id,
    host_id,
    is_superhost,
    host_since,
    neighbourhood,
    room_type,
    property_type,
    latitude,
    longitude,
    accommodates,
    bedrooms,
    beds,
    nightly_price,
    minimum_nights,
    first_review_date,
    last_review_date,
    review_scores_rating,
    city,
    snapshot_date as sourced_from_snapshot
from ranked
where rn = 1
