from os.path import exists
import numpy as np
import requests
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pandas as pd
import psycopg2
import pickle
from os import environ

OWNER = 'CSSEGISandData'
REPO = 'COVID-19'
PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'

relabel = {
    'Last Update': 'Last_Update',
    'Country/Region': 'Country_Region',
    'Province/State': 'Province_State',
}

user = environ['PGUID']
pwd = environ['PGPASS']


def refactor_df(dataframe):
    for label in dataframe:
        if label in relabel:
            df = df.rename(columns={label: relabel[label]})

    labels = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered', 'Active']

    for label in labels:
        if label not in dataframe:
            dataframe[label] = np.nan
    return dataframe[labels]


if not exists("dataframes.pickle"):
    download_urls = []
    response = requests.get(URL)
    for data in tqdm(response.json()):
        if data['name'].endswith('.csv'):
            download_urls.append(data['download_url'])

    csv_list = []
    for csv in download_urls[:10]:  # change this if you want to import more data
        csv_list.append(refactor_df(pd.read_csv(csv)))

    with open('dataframes.pickle', 'wb') as file:
        pickle.dump(csv_list, file, protocol=pickle.HIGHEST_PROTOCOL)
else:
    with open('dataframes.pickle', 'rb') as handle:
        csv_list = pickle.load(handle)


def insert_to_postgres(dataframes_list):
    rows_imported = 0
    for df in dataframes_list:
        try:
            engine = create_engine(f'postgresql://{user}:{pwd}@localhost:5432/Adventure')
            # save df to postgres
            df.to_sql('example', engine, if_exists='append', index=False)
            rows_imported += len(df)
            print(f"Data imported successful: {rows_imported}")
        except Exception as e:
            print("Data load error: " + str(e))


if __name__ == '__main__':
    insert_to_postgres(csv_list)
