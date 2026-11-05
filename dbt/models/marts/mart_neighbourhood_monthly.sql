-- Rate and RevPAR by neighbourhood and month.
--
-- Scoped to rows carrying a rate, so no metric here is nullable. The demand
-- series runs much longer; see mart_neighbourhood_demand_monthly.
--
-- Note: Austin's Inside Airbnb data publishes neighbourhood as a ZIP code
-- rather than a name.

select
    neighbourhood,
    month_start,
    count(distinct listing_id)         as listings,
    min(rate_sourced_from)             as rate_sourced_from,
    round(median(adr), 2)              as median_adr,
    round(avg(estimated_occupancy), 4) as avg_occupancy,
    round(avg(revpar), 2)              as avg_revpar
from {{ ref('fct_listing_month') }}
where has_rate
  and not is_partial_month
group by 1, 2
having count(distinct listing_id) >= 5
