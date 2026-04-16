SELECT DISTINCT
    lga_name,
    lga_code
FROM {{ ref('stg_lga_code') }}
WHERE lga_name IS NOT NULL AND lga_code IS NOT NULL
