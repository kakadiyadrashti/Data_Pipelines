with months as (
  select distinct date_trunc('month', scraped_date)::date as month_date
  from {{ ref('stg_airbnb_listings') }}
  where scraped_date is not null
)
select
  month_date,
  extract(year from month_date)::int  as year,
  extract(month from month_date)::int as month
from months
