from multiprocessing import context
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import requests
import os
from airflow.models import Variable

default_args = {
    'owner': 'Andriy',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def send_telegram_message(context):
    try:
        token = Variable.get("telegram_bot_token") 
        chat_id = Variable.get("telegram_chat_id")
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        dag_id = context.get('task_instance').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = context.get('task_instance').end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"🚀 DAG: {dag_id}\n✅ Task: {task_id} успішно завершена!\n📅 Час: {execution_date}"
        
        response = requests.post(url, data={'chat_id': chat_id, 'text': message})
        print(f"Telegram response: {response.text}")
    except Exception as e:
        print(f"Помилка відправки в Telegram: {e}")

def send_failure_alert(context):
    try:
        token = Variable.get("telegram_bot_token") 
        chat_id = Variable.get("telegram_chat_id")
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        
        dag_id = context.get('task_instance').dag_id
        task_id = context.get('task_instance').task_id
        error_msg = context.get('exception') 
        
        message = f"❌ ПОМИЛКА В DAG: {dag_id}\n🔺 Task: {task_id} ВПАЛА!\n⚠️ Помилка: {error_msg}"
        
        requests.post(url, data={'chat_id': chat_id, 'text': message})
    except Exception as e:
        print(f"Помилка сповіщення про збій: {e}")
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
        --env-file /opt/airflow/dags/.env \
        -v //c/crypto-data-pipeline/dbt_project:/usr/app/dbt \
        -e DBT_PROFILES_DIR=/usr/app/dbt \
        --entrypoint /bin/bash \
        ghcr.io/dbt-labs/dbt-postgres:1.7.3 -c "dbt run"
        """
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        docker run --rm \
        --network crypto-data-pipeline_default \
        --env-file /opt/airflow/dags/.env \
        -v //c/crypto-data-pipeline/dbt_project:/usr/app/dbt \
        -e DBT_PROFILES_DIR=/usr/app/dbt \
        --entrypoint /bin/bash \
        ghcr.io/dbt-labs/dbt-postgres:1.7.3 -c "dbt test"
        """,
        on_success_callback=send_telegram_message,
        on_failure_callback=send_failure_alert
    )

    fetch_data >> clean_db >> dbt_run >> dbt_test