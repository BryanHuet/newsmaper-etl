import airflow
import pandas as pd
import numpy as np
import requests
import datetime
import logging
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task

COLUMNS = ['id_country','id_source', 'id_date', 'title', 'link', 'description', 'media']
REFERENCES = pd.read_json('/opt/airflow/dags/files/references.json')

def is_in_base(row, base, columns):
    return (row[columns] == base[columns]).all(axis=1).any()
def get_from_rss(rss, id):
   
    r = requests.get(rss)
    soup = BeautifulSoup(r.content,'xml')
    articles = []
    for element in soup.find_all('item'):
       
        media = element.find('content')
        if not media:
            media = element.find('enclosure')

        articles.append({
            'title':element.title.text,
            'link':element.link.text,
            'description':element.description.text,
            'date':element.find('pubDate').text,
            'media':'null' if not media else media['url'],
            'id_source': id,
        })
    return articles

def search_id(row):
    requ = f"SELECT id FROM date WHERE hours={row['hours']} and day={row['day']} and month={row['month']}"
    engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
    with engine.connect() as conn:
        result = conn.execute(text(requ))
    row['id_date'] = int(result.fetchall()[0][0])
    return row

def defineUTC(x):
    try:
        dt = datetime.strptime(x, '%a, %d %b %Y %H:%M:%S %z')
    except:
        try:
            dt = datetime.strptime(x, '%a, %d %b %Y %H:%M:%S %Z')
        except:
            dt = datetime.strptime(x, '%a, %d %b %y %H:%M:%S %z')
    return dt

def explodeDateRow(x):
    x['year'] = x['date'].year
    x['month'] = x['date'].month
    x['day']=x['date'].day
    x['hours']=x['date'].hour
    x['minutes']=x['date'].minute
    x['seconds']=x['date'].second
    return x

def searchCountryFromTxt(txt, references, default_id = 1):
    txtCleaned = txt.replace(',',' ').replace(';',' ').replace('\'',' ').lower()
    txtListed = txtCleaned.split(' ')
    countries = []
    for word in txtListed:
        found = references.loc[
            references.words.apply(
                lambda row: str(word) in row
                )
            ]
        if len(found) > 0:
            countries.append(found.id.iloc[0])
    if len(countries) < 1:
        return default_id
    return pd.DataFrame(countries).value_counts().idxmax()[0]
def applyReferences(row, references):
    return searchCountryFromTxt(row.title + ' ' +row.description, references)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7)
}

with DAG(
    dag_id="etl_newsmaper_new_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=20),
    catchup=False,
    tags=["dev"]
) as dag:
    

    @task
    def extract():
        engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM sources"))

        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        sources = [item for _, item in df[['rss', 'id']].T.to_dict().items()]

        articles = []
        for source in sources:
            articles += get_from_rss(**source)
        return pd.DataFrame(articles)
    
    @task
    def extractDate(data):
        dates = data['date'].apply(defineUTC)
        dates = dates.to_frame().apply(explodeDateRow, axis=1)
        data = data.join(dates,rsuffix='_other')
        return data.apply(search_id, axis=1)

    @task 
    def transform(data):
        data = data.rename(columns={'source':'id_source'})
        data['id_country'] = data.apply(applyReferences,
                                        axis=1,
                                        references=REFERENCES)
        return data[COLUMNS]

    @task
    def load(data):
        logging.info(data.columns)
        engine = sqlalchemy.create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM news")).fetchall()
        
        database = pd.DataFrame(result)
        if (len(database) !=0):
            tested_columns = ['id_source', 'id_date', 'title']
            row_exists = data.apply(is_in_base, base=database, columns=tested_columns, axis=1)
            data = data[~row_exists]

        logging.info(f'load {len(data)} new rows.')
        data.to_sql(name='news', con=engine, if_exists='append', index=False)
    load(transform(extractDate(extract())))