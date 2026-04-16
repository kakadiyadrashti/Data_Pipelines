SELECT DISTINCT
  trim(payload->>'SUBURB_NAME') AS suburb_name,
  (
    SELECT e.value
    FROM jsonb_each_text(s.payload) AS e
    WHERE e.key ~* 'lga.*name'
    LIMIT 1
  ) AS lga_name
FROM bronze.lga_suburb AS s
WHERE s.payload ? 'SUBURB_NAME'
ORDER BY suburb_name;
