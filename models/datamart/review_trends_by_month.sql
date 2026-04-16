SELECT
    DATE_TRUNC('month', f.scraped_date) AS month,
    ROUND(AVG(f.review_scores_rating), 2) AS avg_rating,
    COUNT(f.listing_id) AS total_reviews
FROM {{ ref('fact_airbnb_listings') }} f
WHERE f.review_scores_rating IS NOT NULL
GROUP BY 1
ORDER BY month ASC
