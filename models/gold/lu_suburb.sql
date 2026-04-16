SELECT DISTINCT
    suburb_name,
    lga_name
FROM {{ ref('stg_lga_suburb') }}
WHERE suburb_name IS NOT NULL
