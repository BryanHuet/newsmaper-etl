import airflow
import os
import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task

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

    create_sources_table = PostgresOperator(
        task_id="create_sources_table",
        postgres_conn_id="postgres_localhost",
        sql='sql/sources.sql'
    )
    create_date_table = PostgresOperator(
        task_id="create_date_table",
        postgres_conn_id="postgres_localhost",
        sql='sql/date.sql'
    )

    @task
    def populate_sources_table():
        engine = sqlalchemy.create_engine(
            'postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        data = pd.read_csv('/opt/airflow/dags/files/sources.csv')
        data.to_sql(name='sources', con=engine,
                    if_exists='append', index=False)
    create_countries_table >> populate_countries_table
    create_sources_table >> populate_sources_table()
    create_date_table
    create_news_table
