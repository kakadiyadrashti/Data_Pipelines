CREATE OR REPLACE VIEW bronze.lga_mapping_simple AS
SELECT DISTINCT ON (lga_code)
  NULLIF(regexp_replace(payload->>'LGA_CODE', '[^0-9]', '', 'g'), '')::int AS lga_code,
  trim(payload->>'LGA_NAME') AS lga_name
FROM bronze.lga_mapping
WHERE trim(payload->>'LGA_NAME') <> ''
  AND NULLIF(regexp_replace(payload->>'LGA_CODE', '[^0-9]', '', 'g'), '') IS NOT NULL
ORDER BY lga_code, lga_name;

SELECT * FROM bronze.lga_mapping_simple ORDER BY lga_code;


