{{ config(materialized='view', schema='silver_datamart') }}

with base as (
  select
      date_trunc('month', f.scraped_date)::date                        as month_date
    , to_char(date_trunc('month', f.scraped_date), 'YYYY-MM')          as month_year
    , f.property_type
    , f.room_type
    , f.price::numeric                                                 as price
    , f.review_scores_rating::numeric                                  as review_scores_rating
    , coalesce(f.availability_30, 0)                                   as availability_30
    , (coalesce(f.availability_30,0) > 0)                              as is_active
    , f.host_id
    , coalesce( (f.host_is_superhost::boolean), false )                as host_is_superhost_bool
    , greatest(0, least(30, 30 - coalesce(f.availability_30,0)))       as stays_est
    , f.price::numeric * greatest(0, (30 - coalesce(f.availability_30,0))) as est_revenue
  from {{ ref('fact_airbnb_listings') }} f
  where f.property_type is not null
)
, agg as (
  select
      property_type, room_type, month_date,
      to_char(month_date, 'YYYY-MM')                                   as month_year,
      count(*)                                                         as total_listings,
      sum(is_active::int)                                              as active_listings,
      (sum(is_active::int)::numeric / nullif(count(*),0))              as active_listings_rate,
      min(price)                         FILTER (WHERE is_active)      as min_price_active,
      max(price)                         FILTER (WHERE is_active)      as max_price_active,
      percentile_cont(0.5) within group (order by price)
                                         FILTER (WHERE is_active)      as median_price_active,
      avg(price)                        FILTER (WHERE is_active)       as avg_price_active,
      count(distinct host_id)                                          as distinct_hosts,
      avg( (host_is_superhost_bool::int)::numeric ) FILTER (WHERE is_active) as superhost_rate,
      avg(review_scores_rating)        FILTER (WHERE is_active)        as avg_rating_active,
      sum(stays_est)                                                   as total_stays,
      avg(est_revenue)                 FILTER (WHERE is_active)        as avg_est_rev_per_active
  from base
  group by property_type, room_type, month_date
)
select
    a.*
  , (
      (a.active_listings::numeric
       - lag(a.active_listings) over (partition by a.property_type, a.room_type order by a.month_date))
      / nullif(lag(a.active_listings) over (partition by a.property_type, a.room_type order by a.month_date), 0)
    ) as pct_change_active
  , (
      ((a.total_listings - a.active_listings)::numeric
       - lag(a.total_listings - a.active_listings) over (partition by a.property_type, a.room_type order by a.month_date))
      / nullif(lag(a.total_listings - a.active_listings) over (partition by a.property_type, a.room_type order by a.month_date), 0)
    ) as pct_change_inactive
from agg a
order by property_type, room_type, month_year