from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import tempfile
import os

BUCKET = "enterprise-db-stage"
S3_KEY = "datasets/source_erp/PX_CAT_G1V2.csv"
TABLE = "stage.erp_px_cat_g1v2"

def load_csv_to_postgres(s3_key, table):
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    pg_hook = PostgresHook(postgres_conn_id="postgres_enterprise")

    # # Create a proper temporary file path
    # tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    # tmp_file_path = tmp_file.name
    # tmp_file.close()  # Close so S3Hook can write to it

    # # Download CSV directly to this file
    # s3_hook.download_file(key=s3_key, bucket_name=BUCKET, local_path=tmp_file_path)
    # print(f"Downloaded file to {tmp_file_path}, size={os.path.getsize(tmp_file_path)} bytes")

    # # Load into Postgres
    # conn = pg_hook.get_conn()
    # cursor = conn.cursor()
    # with open(tmp_file_path, "r") as f:
    #     cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)
    # conn.commit()
    # cursor.close()
    # conn.close()
    # print(f"Loaded {s3_key} → {table}")

    # # Cleanup temp file
    # os.remove(tmp_file_path)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        downloaded_file = s3_hook.download_file(key=s3_key, bucket_name=BUCKET, local_path=tmp_dir)
        file_path = os.path.join(tmp_dir, downloaded_file)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        with open(file_path, "r") as file:
            cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", file)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Loaded {s3_key} → {table}")

def check_row_count(table):
    pg_hook = PostgresHook(postgres_conn_id="postgres_enterprise")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table};")
    row_count = cursor.fetchone()[0]
    print(f"Validation: Table {table} contains {row_count} rows")
    cursor.close()
    conn.close()

with DAG(
    dag_id="load_erp_px_cat_g1v2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "stage", "erp"],
) as dag:

    load_task = PythonOperator(
        task_id="load_erp_px_cat_g1v2",
        python_callable=load_csv_to_postgres,
        op_kwargs={"s3_key": S3_KEY, "table": TABLE},
    )

    validate_task = PythonOperator(
        task_id="validate_erp_px_cat_g1v2",
        python_callable=check_row_count,
        op_kwargs={"table": TABLE},
    )

    load_task >> validate_task
    
    