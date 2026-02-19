
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import requests
import os
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

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

def alert_on_price_jump(**context):
    try:
        data = context['ti'].xcom_pull(task_ids='extract_task')
        current_price = float(data['price'])
        symbol = data['symbol']
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        query = f"""
            SELECT price FROM public.fct_crypto_trends 
            WHERE symbol = '{symbol}' 
            ORDER BY event_time DESC 
            LIMIT 1 OFFSET 1
        """
        result = pg_hook.get_first(query)
        
        if result:
            previous_price = float(result[0])
            price_change = ((current_price - previous_price) / previous_price) * 100
            
            if price_change >= 5:
                token = Variable.get("telegram_bot_token")
                chat_id = Variable.get("telegram_chat_id")
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                
                message = (
                    f"🚀 *MOON ALERT!* 🚀\n\n"
                    f"Пара: {symbol}\n"
                    f"📈 Ріст: +{price_change:.2f}%\n"
                    f"💰 Нова ціна: ${current_price:,.2f}\n"
                    f"📉 Попередня: ${previous_price:,.2f}"
                )
                
                requests.post(url, data={'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'})
                print(f"Alert sent for {symbol}!")
            else:
                print(f"Change is {price_change:.2f}%, no alert needed.")
        else:
            print("No previous data found to compare.")
            
    except Exception as e:
        print(f"Помилка в аналізі ціни: {e}")

default_args = {
    'owner': 'Andriy',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_failure_alert,
}

with DAG(
    dag_id='crypto_ingestion_v1',
    default_args=default_args,
    start_date=datetime(2026, 2, 13),
    schedule_interval='*/5 * * * *', 
    catchup=False
) as dag:
    
    extract_task = BashOperator(
        task_id='extract_to_minio',
        bash_command='python /opt/airflow/data_ingestion/main.py extract',
    )

    load_task = BashOperator(
        task_id='load_to_postgres',
        bash_command='python /opt/airflow/data_ingestion/main.py load',
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
    )
    price_alert_task = PythonOperator(
    task_id='price_alert_task',
    python_callable=alert_on_price_jump,
    provide_context=True
)

    extract_task >> load_task >> price_alert_task >> clean_db >> dbt_run >> dbt_test