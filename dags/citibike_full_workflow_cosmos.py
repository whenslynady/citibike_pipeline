import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ------------------------------
# Cosmos imports
# ------------------------------
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# ==========================================================
# ENVIRONMENT VARIABLES
# ==========================================================
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DATA_FOLDER = os.path.join(AIRFLOW_HOME, "data")
os.makedirs(DATA_FOLDER, exist_ok=True)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "de-citibike-data-pipeline")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_de-citibike-data-pipeline")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "de_citibike_tripdata_1")
GCP_CONN_ID = os.environ.get("GCP_CONN_ID", "google_cloud_default")

dataset_zip = "202501-citibike-tripdata.zip"
dataset_url = f"https://s3.amazonaws.com/tripdata/{dataset_zip}"

GOOGLE_APPLICATION_CREDENTIALS = "/opt/airflow/.google/credentials/google_credentials.json"
if not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
    raise FileNotFoundError(f"❌ Credential file not found at {GOOGLE_APPLICATION_CREDENTIALS}")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# ==========================================================
# ETL FUNCTIONS
# ==========================================================
def extract_local_data(**context):
    """Download and extract Citibike ZIP file into the local Airflow data folder."""
    import subprocess
    subprocess.run(["curl", "-sSL", dataset_url, "-o", f"{DATA_FOLDER}/{dataset_zip}"], check=True)
    subprocess.run(["unzip", "-o", f"{DATA_FOLDER}/{dataset_zip}", "-d", DATA_FOLDER], check=True)
    logging.info("✅ Data extracted successfully.")

def validate_raw_data(**context):
    """Ensure raw CSV files exist before proceeding with the transformation."""
    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]
    if not files:
        raise ValueError("❌ No CSV found for validation.")
    logging.info(f"📁 {len(files)} CSV file(s) ready for transformation.")

def convert_to_parquet(**context):
    """Convert all CSV files to Parquet, adjust types, and add a date partition column."""
    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]
    if not files:
        logging.error("❌ No CSV files found in data folder.")
        return

    for csv_file in files:
        src_file = os.path.join(DATA_FOLDER, csv_file)
        try:
            df = pd.read_csv(src_file)

            # Convert datetime fields
            for col in ["started_at", "ended_at"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Convert station IDs to string
            for col in ["start_station_id", "end_station_id"]:
                if col in df.columns:
                    df[col] = df[col].astype(str)

            # Create partition column
            if "started_at" in df.columns:
                df["started_at_date"] = df["started_at"].dt.date

            # Write to Parquet
            pq_file = os.path.join(DATA_FOLDER, csv_file.replace(".csv", ".parquet"))
            table = pa.Table.from_pandas(df)
            pq.write_table(table, pq_file)
            logging.info(f"✅ Parquet created: {pq_file}")

        except Exception as e:
            logging.error(f"⚠️ Error converting {csv_file}: {e}")

    logging.info("✅ All CSV files converted to Parquet.")

def upload_to_gcs(**context):
    """Upload Parquet files to Google Cloud Storage."""
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    bucket = gcs_hook.get_bucket(BUCKET)
    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".parquet")]
    for pq_file in files:
        blob = bucket.blob(f"raw/{pq_file}")
        blob.upload_from_filename(os.path.join(DATA_FOLDER, pq_file))
        logging.info(f"✅ Uploaded: {pq_file} → gs://{BUCKET}/raw/")

def verify_gcs_upload(**context):
    """Verify that Parquet files are successfully uploaded to GCS."""
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    files = gcs_hook.list(BUCKET, prefix="raw/")
    if not files:
        raise ValueError("❌ No files found in GCS.")
    logging.info(f"✅ GCS verification successful: {len(files)} file(s) found.")

# ==========================================================
# BIGQUERY QUERIES
# ==========================================================
CREATE_EXTERNAL_TABLE_QUERY = f"""
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.external_citibike_tripdata_2025`
OPTIONS(
    format = 'PARQUET',
    uris = ['gs://{BUCKET}/raw/*.parquet']
);
"""

CREATE_PARTITIONED_TABLE_QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.citibike_tripdata_partitioned_2025`
PARTITION BY started_at_date AS
SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.external_citibike_tripdata_2025`;
"""

# ==========================================================
# COSMOS / DBT CONFIG
# ==========================================================
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="citibike_dbt_gcp",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/.dbt/profiles.yml"
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt"
)

DBT_RENDER_CONFIG = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=["path:models"],
    dbt_executable_path="/usr/local/bin/dbt"
)

# ==========================================================
# DAG DEFINITION
# ==========================================================
default_args = {
    "owner": "andy",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="citibike_full_workflow_cosmos",
    default_args=default_args,
    description="Full ETL + BigQuery + DBT (Cosmos) workflow for Citibike",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "etl", "gcs", "bigquery", "dbt", "cosmos"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ================================
    # ETL Tasks
    # ================================
    extract = PythonOperator(task_id="extract_local_data", python_callable=extract_local_data)
    validate = PythonOperator(task_id="validate_raw_data", python_callable=validate_raw_data)
    to_parquet = PythonOperator(task_id="convert_to_parquet", python_callable=convert_to_parquet)
    upload = PythonOperator(task_id="upload_to_gcs", python_callable=upload_to_gcs)
    verify = PythonOperator(task_id="verify_gcs_upload", python_callable=verify_gcs_upload)

    # ================================
    # BigQuery Tasks
    # ================================
    create_external_table = BigQueryInsertJobOperator(
        task_id="create_bigquery_dataset",
        configuration={"query": {"query": CREATE_EXTERNAL_TABLE_QUERY, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )

    load_partitioned_table = BigQueryInsertJobOperator(
        task_id="load_data_to_bigquery",
        configuration={"query": {"query": CREATE_PARTITIONED_TABLE_QUERY, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )

    # ================================
    # DBT TaskGroup (Cosmos)
    # ================================
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        render_config=DBT_RENDER_CONFIG,
    )

    # ================================
    # DAG Dependencies
    # ================================
    start >> extract >> validate >> to_parquet >> upload >> verify
    verify >> create_external_table >> load_partitioned_table >> dbt_transform >> end
