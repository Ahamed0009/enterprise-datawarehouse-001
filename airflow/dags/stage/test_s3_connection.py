from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


def test_s3_connection():
    hook = S3Hook(aws_conn_id="aws_s3_conn")

    bucket_name = "enterprise-db-stage"
    prefix = "datasets/source_crm"

    files = hook.list_keys(
        bucket_name=bucket_name,
        prefix=prefix
    )

    print("Files found in S3:")
    for f in files:
        print(f)


with DAG(
    dag_id="test_s3_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "test"]
) as dag:

    test_connection = PythonOperator(
        task_id="test_s3_access",
        python_callable=test_s3_connection
    )
    