SELECT
    suburb_name,
    lga_name
FROM bronze.lga_suburb_clean
WHERE suburb_name IS NOT NULL
