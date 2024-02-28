import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy import text
from airflow.decorators import task


def get_from_rss(rss, id):
    r = requests.get(rss)
    soup = BeautifulSoup(r.content, 'xml')
    articles = []
    for element in soup.find_all('item'):

        media = element.find('content')
        if not media:
            media = element.find('enclosure')

        articles.append({
            'title': element.title.text,
            'link': element.link.text,
            'description': element.description.text,
            'date': element.find('pubDate').text,
            'media': 'null' if not media else media['url'],
            'id_source': id,
        })
    return articles


@task
def extractFromRss():
    engine = sqlalchemy.create_engine(
        'postgresql://airflow:airflow@host.docker.internal:5432/airflow')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM sources"))

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    sources = [item for _, item in df[['rss', 'id']].T.to_dict().items()]

    articles = []
    for source in sources:
        articles += get_from_rss(**source)
    return pd.DataFrame(articles)
