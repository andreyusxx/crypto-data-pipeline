from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='db_full_reset', 
    start_date=datetime(2026, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=['maintenance', 'database']
) as dag:

    reset_tables = PostgresOperator(
        task_id='truncate_all_prices',
        postgres_conn_id='crypto_db_conn',
        sql="""
            TRUNCATE TABLE 
                prices_btc, prices_eth, prices_sol, prices_bnb, 
                prices_ada, prices_doge, prices_xrp, prices_dot 
            RESTART IDENTITY;
        """
    )