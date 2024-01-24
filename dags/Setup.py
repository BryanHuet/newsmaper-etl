import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "setup_newsmaper_pipeline"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7)
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=["dev"]
) as dag:

    create_countries_table = PostgresOperator(
            task_id="create_countries_table",
            postgres_conn_id="postgres_localhost",
            sql='sql/countries.sql',
        )

    create_news_table = PostgresOperator(
            task_id="create_news_table",
            postgres_conn_id="postgres_localhost",
            sql='sql/news.sql',
        )

    populate_countries_table = PostgresOperator(
        task_id="populate_countries_table",
        postgres_conn_id="postgres_localhost",
        sql='sql/sample_countries.sql'
    )

    create_countries_table >> create_news_table >> populate_countries_table
