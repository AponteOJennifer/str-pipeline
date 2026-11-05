-- Latest known attributes for every listing ever observed.
--
-- Separate from dim_listings on purpose. dim_listings describes the market as
-- it stands now, so it reads from the most recent complete snapshot only. This
-- model has a different job: give the fact table attributes for any listing it
-- has demand for, including listings that have since been delisted and
-- listings that only ever appeared in an incomplete snapshot.
--
-- Caveat worth stating: attributes are as-of last observation, applied across
-- that listing's whole history. A listing that was re-categorised or re-priced
-- carries its final attributes backwards. For neighbourhood and room type that
-- is stable enough; it would not be acceptable for a rate, which is why rate
-- is looked up as-of the month instead.

with ranked as (
    select
        l.*,
        row_number() over (
            partition by listing_id order by snapshot_date desc
        ) as rn,
        min(snapshot_date) over (partition by listing_id) as first_seen_snapshot,
        max(snapshot_date) over (partition by listing_id) as last_seen_snapshot
    from {{ ref('stg_listings') }} l
)

select
    listing_id,
    neighbourhood,
    room_type,
    accommodates,
    minimum_nights,
    first_seen_snapshot,
    last_seen_snapshot
from ranked
where rn = 1
