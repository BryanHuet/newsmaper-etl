import airflow
import pandas as pd
import numpy as np
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import sqlalchemy

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7)
}

with DAG(
    dag_id="setup_newsmaper_pipeline",
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=["dev"]
) as dag:
    
    @task
    def create_table(schema):
        PostgresOperator(
            task_id="create_countries_table",
            dag=dag,
            postgres_conn_id="postgres_default",
            sql=schema,
        )
    @task 
    def create_table_news(schema):
        PostgresOperator(
            task_id="create_news_table",
            dag=dag,
            postgres_conn_id="postgres_default",
            sql=schema,
        )

    @task 
    def fill_countries():
        engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        df = pd.DataFrame([{'name' : 'France'}, {'name' : 'Italie'}])
        logging.info(df)
        df.to_sql(name='countries', con=engine, if_exists='replace')
    create_table("sql/countries.sql") >> fill_countries() >> create_table_news("sql/news.sql")