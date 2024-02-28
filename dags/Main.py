import airflow
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import text
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from Load import *
from Extract import *
from Transform import *


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7)
}

with DAG(
    dag_id="newsmaper_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=20),
    catchup=False,
    tags=["dev"]
) as dag:

    @task
    def drop_duplicate():
        tested_columns = ['id_source', 'id_date', 'title']
        engine = sqlalchemy.create_engine(
            'postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM news")).fetchall()   
        database = pd.DataFrame(
            result, index=np.array(result)[:, 0])[tested_columns]
        if len(database) == 0:
            return
        no_duplicates = database.drop_duplicates()
        if len(no_duplicates) == len(database):
            return
        duplicates = database.loc[database.index.drop(no_duplicates.index)]
        with engine.connect() as conn:
            for i in range(0, len(duplicates)):
                row = duplicates.iloc[i]
                sql = f"DELETE FROM news WHERE id={row.name}"
                conn.execute(text(sql))
    drop_duplicate() >> load(
        renameAndSelectColumns(findCountry(explodeDate(extractFromRss()))))