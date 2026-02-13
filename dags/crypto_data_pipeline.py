from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Andriy',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_ingestion_v1',
    default_args=default_args,
    start_date=datetime(2026, 2, 13),
    schedule_interval='*/5 * * * *', 
    catchup=False
) as dag:

    fetch_data = BashOperator(
        task_id='fetch_crypto_prices',
        bash_command='python /opt/airflow/data_ingestion/main.py once'
    )

    clean_db = PostgresOperator(
        task_id='clean_old_data',
        postgres_conn_id='crypto_db_conn',
        sql='CALL public.clean_old_data();'
    )

    fetch_data >> clean_db