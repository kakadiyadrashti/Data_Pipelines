{{ config(materialized='view', schema='silver_datamart') }}

with fct as (
  select
      date_trunc('month', f.scraped_date)::date                        as month_date
    , to_char(date_trunc('month', f.scraped_date), 'YYYY-MM')          as month_year
    , f.suburb_name                                                    as suburb_name
    , f.host_id
    , f.price::numeric                                                 as price
    , coalesce(f.availability_30, 0)                                   as availability_30
    , (coalesce(f.availability_30,0) > 0)                              as is_active
    , f.price::numeric * greatest(0, (30 - coalesce(f.availability_30,0))) as est_revenue
  from {{ ref('fact_airbnb_listings') }} f
  where f.suburb_name is not null
)
, map as (
  select lower(suburb_name) as suburb_name_lc, lga_name
  from {{ ref('stg_lga_suburb') }}
)
, base as (
  select
      coalesce(m.lga_name, 'UNKNOWN')                                  as host_neighbourhood_lga
    , month_date
    , to_char(month_date, 'YYYY-MM')                                   as month_year
    , host_id
    , is_active
    , est_revenue
  from fct
  left join map m
    on lower(fct.suburb_name) = m.suburb_name_lc
)
select
    host_neighbourhood_lga
  , month_year
  , count(distinct host_id) * 11000                                    as distinct_hosts_x11000
  , avg(est_revenue) FILTER (WHERE is_active) * 11000                  as avg_est_revenue_active_x11000
  , ( sum(est_revenue) / nullif(count(distinct host_id),0) ) * 11000   as est_revenue_per_host_x11000
from base
group by host_neighbourhood_lga, month_year
order by host_neighbourhood_lga, month_year