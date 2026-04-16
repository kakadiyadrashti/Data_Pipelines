CREATE VIEW bronze.census_g02_simple AS
SELECT DISTINCT
  NULLIF(regexp_replace(payload->>'LGA_CODE_2016','[^0-9]','','g'),'')::int                AS lga_code,
  NULLIF(regexp_replace(payload->>'Median_age_persons','[^0-9.]','','g'),'')::numeric(10,0)        AS median_age_persons,
  NULLIF(regexp_replace(payload->>'Median_mortgage_repay_monthly','[^0-9.]','','g'),'')::numeric(10,0) AS median_mortgage_repay_monthly,
  NULLIF(regexp_replace(payload->>'Median_tot_prsnl_inc_weekly','[^0-9.]','','g'),'')::numeric(10,0)  AS median_tot_prsnl_inc_weekly,
  NULLIF(regexp_replace(payload->>'Median_rent_weekly','[^0-9.]','','g'),'')::numeric(10,0)           AS median_rent_weekly,
  NULLIF(regexp_replace(payload->>'Median_tot_fam_inc_weekly','[^0-9.]','','g'),'')::numeric(10,0)    AS median_tot_fam_inc_weekly,
  NULLIF(regexp_replace(payload->>'Average_num_psns_per_bedroom','[^0-9.]','','g'),'')::numeric(10,2) AS average_num_psns_per_bedroom,
  NULLIF(regexp_replace(payload->>'Median_tot_hhd_inc_weekly','[^0-9.]','','g'),'')::numeric(10,0)    AS median_tot_hhd_inc_weekly,
  NULLIF(regexp_replace(payload->>'Average_household_size','[^0-9.]','','g'),'')::numeric(10,2)       AS average_household_size
FROM bronze.census_g02;

SELECT *
FROM bronze.census_g02_simple
ORDER BY lga_code
LIMIT 200;

