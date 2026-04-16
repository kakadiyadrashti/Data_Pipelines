1)
SET search_path TO "exactly", public;

WITH kpis AS (
    SELECT
        lga_name,
        month_year,              -- adjust if your date column differs
        estimated_revenue,
        active_listings
    FROM "exactly"."kpis_monthly"
    WHERE month_year >= date_trunc('month', current_date) - INTERVAL '112 months'
),
agg AS (
    SELECT
        lga_name,
        SUM(estimated_revenue)                                      AS total_revenue,
        SUM(active_listings)                                        AS total_active_listings,
        CASE WHEN SUM(active_listings) > 0
             THEN SUM(estimated_revenue)::numeric / NULLIF(SUM(active_listings),0)
             ELSE NULL END                                          AS revenue_per_active_listing
    FROM kpis
    GROUP BY lga_name
),
ranked AS (
    SELECT
        lga_name,
        revenue_per_active_listing,
        RANK() OVER (ORDER BY revenue_per_active_listing DESC NULLS LAST) AS r_desc,
        RANK() OVER (ORDER BY revenue_per_active_listing ASC  NULLS LAST) AS r_asc
    FROM agg
),
picked AS (
    SELECT lga_name, revenue_per_active_listing, 'TOP_13' AS bucket
    FROM ranked WHERE r_desc <= 13
    UNION ALL
    SELECT lga_name, revenue_per_active_listing, 'BOTTOM_13' AS bucket
    FROM ranked WHERE r_asc <= 13
),
joined AS (
    SELECT
        p.bucket,
        p.lga_name,
        p.revenue_per_active_listing,
        COALESCE(c.median_age, c.avg_age)                AS median_age,
        COALESCE(c.household_size, c.avg_household_size) AS household_size
    FROM picked p
    LEFT JOIN "exactly"."lu_lga" c
      ON LOWER(p.lga_name) = LOWER(c.lga_name)
)
SELECT
    bucket,
    COUNT(*)                                           AS lga_count,
    ROUND(AVG(revenue_per_active_listing)::numeric, 2) AS avg_rev_per_active_listing,
    ROUND(AVG(median_age)::numeric, 2)                 AS avg_median_age,
    ROUND(AVG(household_size)::numeric, 2)             AS avg_household_size
FROM joined
GROUP BY bucket
ORDER BY CASE WHEN bucket='TOP_13' THEN 0 ELSE 1 END;


2)
-- Q2: Rank LGAs by performance over last 112 months
-- Shows: 112-month avg revenue/active, latest month value, CAGR, and rank.
-- Replace table/column names if yours differ.

WITH base AS (
  SELECT
    lga_name,
    DATE_TRUNC('month', month_year)::date AS month_year,
    SUM(estimated_revenue)        AS total_revenue,
    SUM(active_listings)          AS total_active_listings,
    CASE
      WHEN SUM(active_listings) > 0
        THEN SUM(estimated_revenue)::numeric / SUM(active_listings)
      ELSE 0
    END AS rev_per_active
  FROM gold.lga_monthly_metrics
  WHERE month_year >= (CURRENT_DATE - INTERVAL '112 months')
  GROUP BY lga_name, DATE_TRUNC('month', month_year)
),

avg_perf AS (
  SELECT
    lga_name,
    AVG(rev_per_active)                         AS avg_rev_per_active_112m
  FROM base
  GROUP BY lga_name
),

latest_month AS (
  SELECT DISTINCT DATE_TRUNC('month', CURRENT_DATE)::date AS latest_m
),

latest_snap AS (
  SELECT b.lga_name, b.rev_per_active AS latest_rev_per_active
  FROM base b
  JOIN latest_month lm
    ON b.month_year = lm.latest_m
),

-- first and last month values to compute CAGR on revenue per active
bounds AS (
  SELECT
    lga_name,
    MIN(month_year) FILTER (WHERE rev_per_active > 0) AS first_m,
    MAX(month_year)                                   AS last_m
  FROM base
  GROUP BY lga_name
),

cagr AS (
  SELECT
    b1.lga_name,
    NULLIF(b1.rev_per_active,0) AS first_val,
    b2.rev_per_active           AS last_val,
    GREATEST(1,
      (EXTRACT(YEAR FROM age(b2.month_year, b1.month_year)) * 12
       + EXTRACT(MONTH FROM age(b2.month_year, b1.month_year)))
    ) / 12.0 AS years_span
  FROM bounds bo
  JOIN base b1 ON b1.lga_name = bo.lga_name AND b1.month_year = bo.first_m
  JOIN base b2 ON b2.lga_name = bo.lga_name AND b2.month_year = bo.last_m
),

cagr_calc AS (
  SELECT
    lga_name,
    CASE
      WHEN first_val IS NULL OR first_val <= 0 OR last_val IS NULL OR years_span <= 0
        THEN NULL
      ELSE POWER(last_val / first_val, 1





3)Q1 — Best listing type for top 15 neighbourhoods

WITH base AS (
  SELECT
    listing_id, host_id, listing_neighbourhood,
    property_type, room_type, accommodates, price,
    has_availability, availability_30, month_label,
    GREATEST(0, LEAST(30, 30 - COALESCE(availability_30,0))) AS booked_nights,
    (GREATEST(0, LEAST(30, 30 - COALESCE(availability_30,0))) * price) AS est_revenue,
    (has_availability IS TRUE) AS is_active
  FROM airbnb_listings_nsw
),
monthly_neigh AS (
  SELECT
    month_label,
    listing_neighbourhood,
    SUM(est_revenue) FILTER (WHERE is_active) AS total_rev,
    COUNT(DISTINCT listing_id) FILTER (WHERE is_active) AS active_listings
  FROM base
  GROUP BY 1,2
),
neigh_rank AS (
  SELECT
    listing_neighbourhood,
    AVG(total_rev / NULLIF(active_listings,0)) AS avg_rev_per_active
  FROM monthly_neigh
  GROUP BY 1
  ORDER BY avg_rev_per_active DESC
  LIMIT 15
),
stays_by_combo AS (
  SELECT
    b.listing_neighbourhood,
    b.property_type,
    b.room_type,
    b.accommodates,
    SUM(b.booked_nights) AS total_booked_nights
  FROM base b
  JOIN neigh_rank n USING (listing_neighbourhood)
  WHERE b.is_active
  GROUP BY 1,2,3,4
),
best_combo AS (
  SELECT DISTINCT ON (listing_neighbourhood)
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_booked_nights
  FROM stays_by_combo
  ORDER BY listing_neighbourhood, total_booked_nights DESC
)
SELECT
  n.listing_neighbourhood   AS neighbourhood,
  n.avg_rev_per_active,
  bc.property_type,
  bc.room_type,
  bc.accommodates,
  bc.total_booked_nights    AS total_booked_nights_12m,
  (bc.total_booked_nights / 3.0)::NUMERIC(12,0) AS est_stays,
  NULL::INT AS listings   -- optional: join counts if needed
FROM neigh_rank n
LEFT JOIN best_combo bc USING (listing_neighbourhood)
ORDER BY n.avg_rev_per_active DESC;

4)Q2 — Multi-listing hosts: concentrated in one LGA vs distributed

WITH base AS (
  SELECT DISTINCT
    host_id,
    listing_id,
    listing_neighbourhood AS lga_name
  FROM airbnb_listings_nsw
  WHERE has_availability IS TRUE
),
span AS (
  SELECT
    host_id,
    COUNT(DISTINCT listing_id) AS n_listings,
    COUNT(DISTINCT lga_name)   AS n_lgas
  FROM base
  GROUP BY host_id
),
multi AS (
  SELECT * FROM span WHERE n_listings > 1
),
tally AS (
  SELECT
    SUM(CASE WHEN n_lgas = 1 THEN 1 ELSE 0 END) AS concentrated,
    SUM(CASE WHEN n_lgas > 1 THEN 1 ELSE 0 END) AS distributed,
    COUNT(*) AS total_multi
  FROM multi
)
SELECT 'concentrated_one_lga' AS category, concentrated AS hosts,
       ROUND(concentrated::DECIMAL/total_multi*100,2) AS pct
FROM tally
UNION ALL
SELECT 'distributed_multi_lga', distributed,
       ROUND(distributed::DECIMAL/total_multi*100,2)
FROM tally;

5)Q3 — Single-listing hosts: revenue ≥ annualised median mortgage (by LGA)

WITH base AS (
  SELECT
    host_id,
    listing_id,
    listing_neighbourhood AS lga_name,
    GREATEST(0, LEAST(30, 30 - COALESCE(availability_30,0))) AS booked_nights,
    price,
    month_label
  FROM airbnb_listings_nsw
  WHERE has_availability IS TRUE
),
est_rev AS (
  SELECT
    host_id,
    listing_id,
    lga_name,
    SUM(booked_nights * price) AS est_revenue_12m
  FROM base
  GROUP BY 1,2,3
),
single_hosts AS (
  SELECT host_id
  FROM est_rev
  GROUP BY host_id
  HAVING COUNT(DISTINCT listing_id) = 1
),
lga_xwalk AS (
  SELECT
    c.lga_name,
    CONCAT('LGA', LPAD(c.lga_code::TEXT, 5, '0')) AS lga_code_2016
  FROM nsw_lga_code c
),
rev_single_with_code AS (
  SELECT
    e.host_id,
    e.lga_name,
    x.lga_code_2016,
    e.est_revenue_12m
  FROM est_rev e
  JOIN single_hosts s USING (host_id)
  LEFT JOIN lga_xwalk x USING (lga_name)
),
mort AS (
  SELECT
    lga_code_2016,
    (median_mortgage_repay_monthly * 12) AS annualised_mortgage
  FROM census_2016_g02_nsw_lga
),
joined AS (
  SELECT
    r.host_id,
    r.lga_code_2016,
    r.lga_name,
    r.est_revenue_12m,
    m.annualised_mortgage,
    (r.est_revenue_12m >= m.annualised_mortgage) AS covers_mortgage
  FROM rev_single_with_code r
  LEFT JOIN mort m USING (lga_code_2016)
)
SELECT
  j.lga_code_2016,
  nc.lga_name              AS LGA_NAME,
  COUNT(DISTINCT j.host_id) AS hosts,
  SUM(CASE WHEN j.covers_mortgage THEN 1 ELSE 0 END) AS covers,
  ROUND(SUM(CASE WHEN j.covers_mortgage THEN 1 ELSE 0 END)::DECIMAL
        / COUNT(DISTINCT j.host_id) * 100, 2) AS pct_cover
FROM joined j
LEFT JOIN nsw_lga_code nc ON CONCAT('LGA', LPAD(nc.lga_code::TEXT, 5, '0')) = j.lga_code_2016
GROUP BY 1,2
ORDER BY pct_cover DESC;



