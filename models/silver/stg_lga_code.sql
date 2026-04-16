SELECT
    (payload ->> 'LGA_NAME') AS lga_name,
    (payload ->> 'LGA_CODE') AS lga_code
FROM bronze.lga_mapping
WHERE payload IS NOT NULL
