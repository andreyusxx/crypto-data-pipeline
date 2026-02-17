from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

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
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        docker run --rm \
        --network crypto-data-pipeline_default \
        -v /c/crypto-data-pipeline/dbt_project:/usr/app/dbt \
        -e DBT_PROFILES_DIR=/usr/app/dbt \
        -e POSTGRES_USER=user \
        -e POSTGRES_PASSWORD=password \
        -e POSTGRES_DB=crypto_db \
        -e POSTGRES_HOST=crypto_postgres \
        ghcr.io/dbt-labs/dbt-postgres:1.7.3 run
        """
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        docker run --rm \
        --network crypto-data-pipeline_default \
        -v /c/crypto-data-pipeline/dbt_project:/usr/app/dbt \
        -e DBT_PROFILES_DIR=/usr/app/dbt \
        -e POSTGRES_USER=user \
        -e POSTGRES_PASSWORD=password \
        -e POSTGRES_DB=crypto_db \
        -e POSTGRES_HOST=crypto_postgres \
        ghcr.io/dbt-labs/dbt-postgres:1.7.3 test    
        """
    )

    fetch_data >> clean_db >> dbt_run >> dbt_test