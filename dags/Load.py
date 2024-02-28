import pandas as pd
import logging
import sqlalchemy
from sqlalchemy import text
from airflow.decorators import task


def is_in_base(row, base, columns):
    return (row[columns] == base[columns]).all(axis=1).any()


@task
def load(data):
    logging.info(data.columns)
    engine = sqlalchemy.create_engine(
        'postgresql://airflow:airflow@host.docker.internal:5432/airflow')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM news")).fetchall()

    database = pd.DataFrame(result)
    if (len(database) != 0):
        tested_columns = ['id_source', 'id_date', 'title']
        row_exists = data.apply(
            is_in_base, base=database, columns=tested_columns, axis=1)
        data = data[~row_exists]

    logging.info(f'load {len(data)} new rows.')
    data.to_sql(name='news', con=engine, if_exists='append', index=False)
