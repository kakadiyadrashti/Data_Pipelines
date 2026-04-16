SELECT
    a.listing_id,
    a.host_id,
    a.property_type,
    a.room_type,
    a.price,
    a.number_of_reviews,
    a.review_scores_rating,
    a.availability_30,
    a.scraped_date,
    h.host_name,
    h.host_is_superhost,
    p.property_type AS property_type_name,
    r.room_type AS room_type_name,
    s.suburb_name,
    s.lga_name,
    l.lga_code
FROM {{ ref('stg_airbnb_listings') }} a
LEFT JOIN {{ ref('lu_host') }} h ON a.host_id = h.host_id
LEFT JOIN {{ ref('lu_property_type') }} p ON a.property_type = p.property_type
LEFT JOIN {{ ref('lu_room_type') }} r ON a.room_type = r.room_type
LEFT JOIN {{ ref('stg_lga_suburb') }} s ON lower(a.listing_neighbourhood) = lower(s.suburb_name)
LEFT JOIN {{ ref('stg_lga_code') }} l ON lower(s.lga_name) = lower(l.lga_name)
WHERE a.price IS NOT NULL
