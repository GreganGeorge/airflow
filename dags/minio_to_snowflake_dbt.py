import os
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.config import RenderConfig
from cosmos.constants import TestBehavior

# Load environment variables
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_SCHEMA_2 = os.getenv("SNOWFLAKE_SCHEMA_2")

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt_banking")

TABLES = ["customers", "accounts"]

profile_config = ProfileConfig(
    profile_name="dbt_banking",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        # Pass additional dbt profile fields here
        profile_args={
            "schema": "ANALYTICS",     # Overrides the schema in Airflow Connection
            "database": "BANKING",  # Optional: ensure it hits the right DB
            "warehouse": "COMPUTE_WH", # Optional: ensure it uses the right compute
        },
    ),
)

# -------- Python Callables --------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True) # makes a directory makedirs can be used to create directory recursively like /tmp/minio_downloads
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key)) # os.path.basename(key) gets the last value after the final / and doing os.path.join is better since to avoid confusion between / and \ betweeN Linux and Windows
            s3.download_file(BUCKET, key, local_file) # downloads the file to local_file
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
    return local_files

def load_to_snowflake(**kwargs): # kwargs gets some values which by default passes to a function like ti.
    hook=SnowflakeHook(snowflake_conn_id='snowflake_conn') 
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")

    if not local_files:
        print("No files found in MinIO.")
        return

    for table in TABLES:
        sql_setup=[
            f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DB}",
            f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}",
            f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA_2}",
            f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table} (data VARIANT)"
        ]
        for query in sql_setup:
            hook.run(query)
    conn=hook.get_conn()
    cur = conn.cursor()

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        for f in files:
            cur.execute(f"PUT file://{f} @%{table}")  # file:// refers that the file is stored in the local hard disk and @%table refers to the stage, @% is used since this refers to the table's stage which is automatically created after table's creation, @stage_name can be used after creating the stage.
            print(f"Uploaded {f} -> @{table} stage")

        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table}")

    cur.close()
    conn.close()

# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake
    )

    transform_data = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        default_args={"retries": 2},
        render_config=RenderConfig(
            select=["path:models", "path:snapshots"], 
            test_behavior=TestBehavior.AFTER_EACH,
        )
    )

    task1 >> task2 >> transform_data