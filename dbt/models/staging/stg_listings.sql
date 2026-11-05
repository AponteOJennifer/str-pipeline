-- One row per listing per snapshot. Listings repeat across snapshots on purpose:
-- that repetition is how price and availability change over time becomes visible.

select
    id                                          as listing_id,
    cast(snapshot_date as date)                 as snapshot_date,
    cast(id as varchar) || '-' || snapshot_date as listing_snapshot_key,

    host_id,
    host_is_superhost = 't'                     as is_superhost,
    try_cast(host_since as date)                as host_since,

    neighbourhood_cleansed                      as neighbourhood,
    room_type,
    property_type,
    latitude,
    longitude,

    accommodates,
    bedrooms,
    beds,

    price                                       as nightly_price,
    minimum_nights,
    maximum_nights,

    -- Inside Airbnb reports forward-looking availability. 365 minus the
    -- available nights is the booked-or-blocked portion of the next year.
    availability_30,
    availability_60,
    availability_90,
    availability_365,

    number_of_reviews,
    number_of_reviews_ltm,
    try_cast(first_review as date)              as first_review_date,
    try_cast(last_review as date)               as last_review_date,
    review_scores_rating,
    reviews_per_month,

    city

from {{ source('raw', 'raw_listings') }}
