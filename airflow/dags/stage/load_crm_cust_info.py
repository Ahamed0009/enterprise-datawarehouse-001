from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import tempfile
import os

BUCKET = "enterprise-db-stage"
S3_KEY = "datasets/source_crm/cust_info.csv"
TABLE = "stage.crm_cust_info"

def load_csv_to_postgres(s3_key, table):
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    pg_hook = PostgresHook(postgres_conn_id="postgres_enterprise")
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
    dag_id="load_crm_cust_info",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["etl","stage","crm"]
) as dag:

    load_task = PythonOperator(
        task_id="load_crm_cust_info",
        python_callable=load_csv_to_postgres,
        op_kwargs={"s3_key": S3_KEY, "table": TABLE}
    )

    validate_task = PythonOperator(
        task_id="validate_crm_cust_info",
        python_callable=check_row_count,
        op_kwargs={"table": TABLE}
    )

    load_task >> validate_task
    
    