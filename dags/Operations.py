import airflow
import pandas as pd
import numpy as np
import requests
import logging
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task


def is_in_base(row, base, columns):
    return (row[columns] == base[columns]).all(axis=1).any()

columns = ['source', 'title', 'country_id', 'link', 'timestamp', 'origin']     

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7)
}

with DAG(
    dag_id="etl_newsmaper_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["dev"]
) as dag:
    
    @task
    def drop_duplicate():
        engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM news")).fetchall()   
        database = pd.DataFrame(result, index=np.array(result)[:,0])[columns]
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

    @task
    def extract():
        url = 'https://newsapi.org/v2/top-headlines?country=fr&apiKey=6a0ba42d4da24db8ba63dd4f04460798'
        response = requests.get(url)
        logging.info(response)
        df = pd.DataFrame(json.loads(response.text)['articles'])
        data = df[['author', 'title', 'url', 'publishedAt']]
        return data
    
    @task
    def transform(data):
        renamed = data.rename(columns={'author':'source', 'url':'link', 'publishedAt':'origin'})
        renamed['country_id'] = 1
        renamed['timestamp'] = datetime.now().isoformat()
        logging.info(renamed.head(5)[['source', 'title']])
        return renamed
    
    @task
    def load(data):
        engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM news")).fetchall()
        
        database = pd.DataFrame(result)
        tested_columns = ['source', 'title', 'link']
        row_exists = data.apply(is_in_base, base=database, columns=tested_columns, axis=1)
        data = data[~row_exists]

        logging.info(f'load {len(data)} new rows.')
        data.to_sql(name='news', con=engine, if_exists='append', index=False)
    drop_duplicate() >> load(transform(extract()))
