{{ config(materialized='table', schema='exactly') }}


WITH parsed AS (
  SELECT
    (payload->>'LISTING_ID')::bigint               AS listing_id,
    (payload->>'HOST_ID')::bigint                  AS host_id,
    (payload->>'HOST_NAME')                        AS host_name,

    --  Safely handle date (DD/MM/YYYY)
    CASE 
      WHEN payload->>'HOST_SINCE' ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
        THEN to_date(payload->>'HOST_SINCE', 'DD/MM/YYYY')
      ELSE NULL
    END                                            AS host_since,

    --  Safely cast boolean fields
    CASE 
      WHEN lower(payload->>'HOST_IS_SUPERHOST') IN ('t','true','1','y','yes') THEN TRUE
      ELSE FALSE
    END                                            AS host_is_superhost,

    (payload->>'HOST_NEIGHBOURHOOD')               AS host_neighbourhood,
    (payload->>'LISTING_NEIGHBOURHOOD')            AS listing_neighbourhood,
    (payload->>'PROPERTY_TYPE')                    AS property_type,
    (payload->>'ROOM_TYPE')                        AS room_type,

    --  Clean PRICE values before numeric cast
    CASE 
      WHEN trim(payload->>'PRICE') = '' THEN NULL
      ELSE replace(replace(replace(payload->>'PRICE', '$',''), ',', ''),' ','')::numeric(12,2)
    END                                            AS price,

    -- Boolean handling for availability
    CASE 
      WHEN lower(payload->>'HAS_AVAILABILITY') IN ('t','true','1','y','yes') THEN TRUE
      ELSE FALSE
    END                                            AS has_availability,

    NULLIF(payload->>'AVAILABILITY_30','')::int    AS availability_30,
    NULLIF(payload->>'NUMBER_OF_REVIEWS','')::int  AS number_of_reviews,
    NULLIF(payload->>'REVIEW_SCORES_RATING','')::numeric(5,2) AS review_scores_rating,
    ingested_at::date                              AS scraped_date
  FROM bronze.airbnb_data
)
SELECT *
FROM parsed
WHERE listing_id IS NOT NULL