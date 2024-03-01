import pandas as pd
import datetime
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from airflow.decorators import task
import logging

COLUMNS = ['id_country', 'id_source', 'id_date',
           'title', 'link', 'description', 'media']
REFERENCES = pd.read_json('/opt/airflow/dags/files/references.json')


def searchCountryFromTxt(txt, references, default_id=1):
    txtCleaned = txt.replace(',', ' ').replace(
        ';', ' ').replace('\'', ' ').lower()
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
    return searchCountryFromTxt(row.title + ' ' + row.description, references)


def search_id(row):
    requ = f"SELECT id FROM date WHERE hours={row['hours']} and day={row['day']} and month={row['month']} and year={row['year']}"
    engine = sqlalchemy.create_engine(
        'postgresql://airflow:airflow@host.docker.internal:5432/airflow')
    with engine.connect() as conn:
        result = conn.execute(text(requ))
    try:
        row['id_date'] = int(result.fetchall()[0][0])
    except:
        logging.info(requ)
        raise ValueError  
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
    x['day'] = x['date'].day
    x['hours'] = x['date'].hour
    x['minutes'] = x['date'].minute
    x['seconds'] = x['date'].second
    return x


@task
def explodeDate(data):
    dates = data['date'].apply(defineUTC)
    dates = dates.to_frame().apply(explodeDateRow, axis=1)
    data = data.join(dates, rsuffix='_other')
    return data.apply(search_id, axis=1)


@task
def findCountry(data):
    data['id_country'] = data.apply(
        applyReferences, axis=1, references=REFERENCES)
    return data


@task 
def renameAndSelectColumns(data):
    data = data.rename(columns={'source': 'id_source'})
    return data[COLUMNS]