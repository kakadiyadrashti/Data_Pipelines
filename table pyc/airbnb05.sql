SELECT current_database() AS db, current_user AS usr;
SELECT schema_name FROM information_schema.schemata ORDER BY 1;

-- List all non-template databases
SELECT datname FROM pg_database
WHERE datistemplate = false
ORDER BY 1;

SELECT schema_name FROM information_schema.schemata ORDER BY 1;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'bronze'
ORDER BY table_name;

SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name = 'bronze';

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.airbnb_data  (
  payload     JSONB,
  src_file    TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.census_g01   (
  payload     JSONB,
  src_file    TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.census_g02   (
  payload     JSONB,
  src_file    TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.lga_mapping  (
  payload     JSONB,
  src_file    TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.lga_suburb   (
  payload     JSONB,
  src_file    TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

SELECT 'airbnb_data', COUNT(*) FROM bronze.airbnb_data
UNION ALL SELECT 'census_g01', COUNT(*) FROM bronze.census_g01
UNION ALL SELECT 'census_g02', COUNT(*) FROM bronze.census_g02
UNION ALL SELECT 'lga_mapping', COUNT(*) FROM bronze.lga_mapping
UNION ALL SELECT 'lga_suburb', COUNT(*) FROM bronze.lga_suburb;

-- pick columns out of the JSON payload
-- Safe, no-errors select from bronze.airbnb_data
SELECT
  NULLIF(payload->>'HOST_ID','')::bigint                           AS host_id,
  payload->>'HOST_NAME'                                            AS host_name,
  payload->>'ROOM_TYPE'                                            AS room_type,
  -- strip currency symbols/commas; empty -> NULL; cast to numeric
  NULLIF(regexp_replace(COALESCE(payload->>'PRICE',''),
                        '[^0-9\.\-]', '', 'g'),'')::numeric(10,2)  AS price,
  NULLIF(payload->>'NUMBER_OF_REVIEWS','')::int                    AS num_reviews,
  NULLIF(payload->>'REVIEW_SCORES_RATING','')::numeric             AS rating,
  payload->>'LISTING_NEIGHBOURHOOD'                                AS listing_neighbourhood,
  payload->>'HOST_NEIGHBOURHOOD'                                   AS host_neighbourhood,
  src_file,
  ingested_at
FROM bronze.airbnb_data
LIMIT 20;


