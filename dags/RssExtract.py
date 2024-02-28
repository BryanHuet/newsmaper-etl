import airflow
import pandas as pd
import numpy as np
import requests
import logging
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task


def is_in_base(row, base, columns):
    return (row[columns] == base[columns]).all(axis=1).any()


def get_from_rss(url, source_name, country_id):
   
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    articles = []
    for element in soup.find_all('item'):
        articles.append({
            'title':element.title.string,
            'origin':element.pubdate.string,
            'link':element.guid.string,
            'source': source_name,
            'country_id':country_id,
            'timestamp':datetime.now().isoformat()
        })
    return articles

columns = ['source', 'title', 'country_id', 'link', 'timestamp', 'origin']     

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7)
}

with DAG(
    dag_id="etl_newsmaper_rss_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=20),
    catchup=False,
    tags=["dev"]
) as dag:
    
    @task
    def extract():

        sources = [
            {
                'url':"https://www.lefigaro.fr/rss/figaro_actualites.xml",
                'source_name':"Le Figaro",
                'country_id':1,
            },
            {
                'url':"https://www.francetvinfo.fr/titres.rss",
                'source_name':"franceinfo",
                'country_id':1,
            },
            {
                'url':"https://www.la-croix.com/RSS/UNIVERS",
                'source_name':"La Croix",
                'country_id':1,
            },
            {
                'url':"https://www.humanite.fr/feed",
                'source_name':"L'Humanité",
                'country_id':1,
            }


        ]

        articles = []
        for source in sources:
            articles += get_from_rss(**source)

        return pd.DataFrame(articles)

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
    drop_duplicate() >> load(extract())