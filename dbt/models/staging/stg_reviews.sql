-- Each snapshot's reviews file contains every review written up to that date,
-- so a review that existed in December appears again in March, June and
-- September. Deduplicating on review id keeps one row per actual review.
--
-- Earliest snapshot wins. A review's content never changes, and taking the
-- earliest keeps reviews belonging to listings that later left the market.

with ranked as (

    select
        id          as review_id,
        listing_id,
        try_cast(date as date) as review_date,
        reviewer_id,
        city,
        cast(snapshot_date as date) as first_seen_snapshot,
        row_number() over (
            partition by id
            order by snapshot_date
        ) as rn
    from {{ source('raw', 'raw_reviews') }}

)

select
    review_id,
    listing_id,
    review_date,
    reviewer_id,
    city,
    first_seen_snapshot
from ranked
where rn = 1
  and review_date is not null
