SELECT
    f.host_id,
    h.host_name,
    h.host_is_superhost,
    COUNT(f.listing_id) AS total_listings,
    SUM(f.number_of_reviews) AS total_reviews
FROM {{ ref('fact_airbnb_listings') }} f
LEFT JOIN {{ ref('lu_host') }} h ON f.host_id = h.host_id
GROUP BY f.host_id, h.host_name, h.host_is_superhost
ORDER BY total_reviews DESC
LIMIT 10
