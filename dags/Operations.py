import airflow
import pandas as pd
import requests
import logging
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task

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
        return renamed
    
    @task
    def load(data):
        engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM news")).fetchall()
        database = pd.DataFrame(result)
        data['is_in'] = data[['source','title','link','origin']].isin(database).apply(sum,1) == 4
        data = data.loc[data['is_in']==False]
        data = data[['source', 'title', 'link', 'origin', 'timestamp', 'country_id']]
        logging.info(f'load {len(data)} new rows.')
        data.to_sql(name='news', con=engine, if_exists='append', index=False)
    load(transform(extract()))
