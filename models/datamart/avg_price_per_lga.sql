SELECT
    l.lga_name,
    s.suburb_name,
    ROUND(AVG(f.price), 2) AS avg_price,
    COUNT(f.listing_id) AS total_listings
FROM {{ ref('fact_airbnb_listings') }} f
LEFT JOIN {{ ref('lu_suburb') }} s 
    ON LOWER(f.suburb_name) = LOWER(s.suburb_name)
LEFT JOIN {{ ref('lu_lga') }} l 
    ON LOWER(s.lga_name) = LOWER(l.lga_name)
WHERE f.price IS NOT NULL
GROUP BY l.lga_name, s.suburb_name
ORDER BY avg_price DESC
