CREATE VIEW bronze.census_g01_simple AS
SELECT
  NULLIF(regexp_replace(payload->>'LGA_CODE_2016', '[^0-9]', '', 'g'), '')::int AS lga_code,
  NULLIF(regexp_replace(payload->>'Tot_P_M',       '[^0-9\-]', '', 'g'), '')::int AS tot_p_m,
  NULLIF(regexp_replace(payload->>'Tot_P_F',       '[^0-9\-]', '', 'g'), '')::int AS tot_p_f,
  NULLIF(regexp_replace(payload->>'Tot_P_P',       '[^0-9\-]', '', 'g'), '')::int AS tot_p_p,
  
FROM bronze.census_g01;

SELECT * FROM bronze.census_g01_simple ORDER BY lga_code NULLS LAST LIMIT 200;

