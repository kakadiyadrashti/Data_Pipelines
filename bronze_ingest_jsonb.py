from datetime import datetime
import io, csv, json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extras import execute_values

POSTGRES_CONN_ID = "postgres_default"
GCS_CONN_ID = "google_cloud_default"

# === Your bucket ===
GCS_BUCKET = "australia-southeast1-bde-ecbafc0c-bucket"

# === Exact objects you showed in screenshots ===
LOADS = [
    {"gcs_object": "raw/airbnb/05_2020.csv",                  "table": "bronze.airbnb_data"},
    {"gcs_object": "raw/census/2016Census_G01_NSW_LGA.csv",   "table": "bronze.census_g01"},
    {"gcs_object": "raw/census/2016Census_G02_NSW_LGA.csv",   "table": "bronze.census_g02"},
    {"gcs_object": "raw/mapping/NSW_LGA_CODE.csv",            "table": "bronze.lga_mapping"},
    {"gcs_object": "raw/mapping/NSW_LGA_SUBURB.csv",          "table": "bronze.lga_suburb"},
]

def load_csv_as_jsonb(gcs_object: str, target_table: str, **_):
    """Read CSV from GCS and insert rows as JSONB into target_table(payload, src_file, ingested_at)."""
    gcs = GCSHook(gcp_conn_id=GCS_CONN_ID)
    pg  = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Download as bytes -> decode -> DictReader
    file_bytes = gcs.download(bucket_name=GCS_BUCKET, object_name=gcs_object)
    text = file_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    rows = []
    for r in reader:
        rows.append((json.dumps(r), gcs_object))  # (payload, src_file)

    if not rows:
        print(f"WARNING: {gcs_object} appears empty; skipping insert.")
        return

    with pg.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO {target_table} (payload, src_file) VALUES %s",
                rows,
                template="(%s::jsonb, %s)"
            )
        conn.commit()
    print(f"âœ… Loaded {len(rows)} rows from {gcs_object} into {target_table}")

with DAG(
    dag_id="bronze_ingest_jsonb",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "jsonb", "gcs", "postgres"],
) as dag:

    # Create schema/tables exactly as you want to query them in DBeaver
    create_tables = PostgresOperator(
        task_id="create_bronze_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE SCHEMA IF NOT EXISTS bronze;

        CREATE TABLE IF NOT EXISTS bronze.airbnb_data (
          payload     JSONB       NOT NULL,
          src_file    TEXT        NULL,
          ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bronze.census_g01 (
          payload     JSONB       NOT NULL,
          src_file    TEXT        NULL,
          ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bronze.census_g02 (
          payload     JSONB       NOT NULL,
          src_file    TEXT        NULL,
          ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bronze.lga_mapping (
          payload     JSONB       NOT NULL,
          src_file    TEXT        NULL,
          ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bronze.lga_suburb (
          payload     JSONB       NOT NULL,
          src_file    TEXT        NULL,
          ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    )

    # Make a load task per file
    tasks = []
    for item in LOADS:
        obj = item["gcs_object"]
        tbl = item["table"]

        t = PythonOperator(
            task_id=f"load__{tbl.replace('.', '_')}__{obj.split('/')[-1].replace('.', '_')}",
            python_callable=load_csv_as_jsonb,
            op_kwargs={"gcs_object": obj, "target_table": tbl},
        )
        create_tables >> t
        tasks.append(t)