from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import tempfile
import os


BUCKET = "enterprise-db-stage"

FILE_TABLE_MAP = {
    "datasets/source_erp/CUST_AZ12.csv": "stage.erp_cust_az12",
    "datasets/source_erp/LOC_A101.csv": "stage.erp_loc_a101",
    "datasets/source_erp/PX_CAT_G1V2.csv": "stage.erp_px_cat_g1v2"
}


def load_csv_to_postgres(s3_key, table):

    print(f"Starting load for {s3_key} → {table}")

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    pg_hook = PostgresHook(postgres_conn_id="postgres_enterprise")

    with tempfile.TemporaryDirectory() as tmp_dir:

        downloaded_file = s3_hook.download_file(
            key=s3_key,
            bucket_name=BUCKET,
            local_path=tmp_dir
        )

        file_path = os.path.join(tmp_dir, downloaded_file)

        print(f"Downloaded file to {file_path}")

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        with open(file_path, "r") as file:

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

        print(f"Loaded {s3_key} → {table}")


with DAG(
    dag_id="s3_to_stage_erp_loader",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "stage", "erp"]
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
        