-- models/silver/stg_airbnb_listings.sql
-- Parse JSON, clean + cast, then dedupe by listing_id + scraped_date.

with src as (
  select payload, ingested_at
  from bronze.airbnb_data
),
base as (
  select
    (payload->>'id')::bigint                                       as listing_id,
    null::bigint                                                   as scrape_id,          -- not present; placeholder
    coalesce( (payload->>'last_scraped')::date
            , (payload->>'scraped_date')::date
            , ingested_at::date )                                  as scraped_date,

    (payload->>'host_id')::bigint                                  as host_id,
    nullif(trim(payload->>'host_name'),'')                         as host_name,
    case when lower(coalesce(payload->>'host_is_superhost','f')) in ('t','true','1','y','yes')
         then 't' else 'f' end                                     as host_is_superhost,

    nullif(trim(payload->>'host_neighbourhood'),'')                as host_neighbourhood,
    coalesce(
      nullif(trim(payload->>'neighbourhood_cleansed'),''),
      nullif(trim(payload->>'neighbourhood'),'')
    )                                                              as listing_neighbourhood,

    nullif(trim(payload->>'property_type'),'')                     as property_type,
    payload->>'room_type'                                          as room_type,
    nullif(payload->>'accommodates','')::int                       as accommodates,

    -- price often has $ and commas; strip them
    nullif(regexp_replace(coalesce(payload->>'price',''), '[^0-9\.]', '', 'g'),'')::numeric(10,2)
                                                                   as price,

    case when lower(coalesce(payload->>'has_availability','f')) in ('t','true','1','y','yes')
         then 't' else 'f' end                                     as has_availability,

    nullif(payload->>'availability_30','')::int                    as availability_30,
    nullif(payload->>'number_of_reviews','')::int                  as number_of_reviews,
    nullif(payload->>'review_scores_rating','')::numeric(5,2)      as review_scores_rating
  from src
),
dedup as (
  select
    *,
    row_number() over (
      partition by listing_id, scraped_date
      order by scraped_date desc
    ) as rn
  from base
)
select *
from dedup
where rn = 1
