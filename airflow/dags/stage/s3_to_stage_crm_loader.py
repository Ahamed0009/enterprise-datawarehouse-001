from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import tempfile
import os


BUCKET = "enterprise-db-stage"

FILE_TABLE_MAP = {
    "datasets/source_crm/cust_info.csv": "stage.crm_cust_info",
    "datasets/source_crm/prd_info.csv": "stage.crm_prd_info",
    "datasets/source_crm/sales_details.csv": "stage.crm_sales_details"
}


def load_csv_to_postgres(s3_key, table):

    print(f"Starting load for {s3_key} → {table}")

    # Initialize hooks
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    pg_hook = PostgresHook(postgres_conn_id="postgres_enterprise")

    # Create temporary directory for file download
    with tempfile.TemporaryDirectory() as tmp_dir:

        # Download file from S3
        downloaded_file = s3_hook.download_file(
            key=s3_key,
            bucket_name=BUCKET,
            local_path=tmp_dir
        )

        # Full path to downloaded file
        full_path = os.path.join(tmp_dir, downloaded_file)

        print(f"Downloaded file to {full_path}")

        # Connect to Postgres
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Load data using COPY (fastest method)
        with open(full_path, "r") as file:
            cursor.copy_expert(
                f"""
                COPY {table}
                FROM STDIN
                WITH CSV HEADER
                """,
                file
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Successfully loaded {s3_key} → {table}")


with DAG(
    dag_id="s3_to_stage_crm_loader",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "stage", "crm"]
) as dag:

    for s3_key, table in FILE_TABLE_MAP.items():

        PythonOperator(
            task_id=f"load_{table.split('.')[-1]}",
            python_callable=load_csv_to_postgres,
            op_kwargs={
                "s3_key": s3_key,
                "table": table
            }
        )
        