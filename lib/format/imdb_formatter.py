import os
import pandas as pd
from datetime import date

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def ingested_imdb_to_formatted(**kwargs):
    file_name = 'imdb.json'
    current_day = date.today().strftime("%Y%m%d")

    #create folder
    IMDB_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/trending_movies/" + current_day + "/" + file_name
    FORMATTED_IMDB_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/imdb/trending_movies/" + current_day + "/"
    if not os.path.exists(FORMATTED_IMDB_FOLDER):
        os.makedirs(FORMATTED_IMDB_FOLDER)

    #format data
    df = pd.read_json(IMDB_PATH)
    df['release_date'] = pd.to_datetime(df['release_date'])
    df.drop('title', inplace=True, axis=1)

    #save formatted data as parquet
    parquet_file_name = file_name.replace(".json", ".snappy.parquet")
    df.to_parquet(FORMATTED_IMDB_FOLDER + parquet_file_name)