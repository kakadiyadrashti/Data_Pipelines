-- Map host_neighbourhood (a suburb name) -> LGA
with hn as (
  select distinct host_neighbourhood
  from {{ ref('stg_airbnb_listings') }}
  where host_neighbourhood is not null
),
map as (
  select lower(suburb_name) as suburb_name_lc, lga_name
  from {{ ref('stg_lga_suburb') }}
)
select
  hn.host_neighbourhood,
  m.lga_name as host_neighbourhood_lga
from hn
left join map m
  on lower(hn.host_neighbourhood) = m.suburb_name_lc
