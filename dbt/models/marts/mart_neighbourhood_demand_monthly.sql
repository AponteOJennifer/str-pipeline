-- Demand by neighbourhood and month, over the full review history.
--
-- No rate columns on purpose. This is the long series, and it is the one a
-- seasonality read or a forecast should be built on.

select
    neighbourhood,
    month_start,
    count(distinct listing_id)         as listings,
    sum(reviews_in_month)              as reviews,
    sum(estimated_stays)               as estimated_stays,
    round(avg(estimated_occupancy), 4) as avg_occupancy
from {{ ref('fct_listing_month') }}
where neighbourhood is not null
  and not is_partial_month
group by 1, 2
having count(distinct listing_id) >= 5
