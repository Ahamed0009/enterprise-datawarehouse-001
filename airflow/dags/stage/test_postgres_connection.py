from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def test_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_enterprise")

    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='stage';")

    rows = cursor.fetchall()

    print("Stage tables found:")
    for r in rows:
        print(r)


with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
) as dag:

    test = PythonOperator(
        task_id="test_postgres",
        python_callable=test_postgres
    )
    
    