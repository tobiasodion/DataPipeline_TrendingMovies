import os
import pandas as pd
from datetime import date

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def ingested_boxoffice_to_formatted(**kwargs):
    file_name = 'boxoffice.csv'
    current_day = date.today().strftime("%Y%m%d")

    #create folder
    BOX_OFFICE_PATH = DATALAKE_ROOT_FOLDER + "raw/boxoffice/box_office_movies/" + current_day + "/" + file_name
    FORMATTED_BOX_OFFICE_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/boxoffice/box_office_movies/" + current_day + "/"
    if not os.path.exists(FORMATTED_BOX_OFFICE_FOLDER):
        os.makedirs(FORMATTED_BOX_OFFICE_FOLDER)
    
    #format data
    df = pd.read_csv(BOX_OFFICE_PATH)
    df['total_revenue'] = df['total_revenue'].str.replace('$','')
    df['theatre_count'] = df['theatre_count'].str.replace('-','0')
    df['title'] = df['title'].str.replace('\n2022 Re-release','')
    df = df.replace(',','', regex=True)
    df['date_total_revenue'] = pd.to_datetime(df['date_total_revenue'])
    df[['total_revenue', 'theatre_count']] = df[['total_revenue', 'theatre_count']].apply(pd.to_numeric)
  
    #save formatted data as parquet
    parquet_file_name = file_name.replace(".csv", ".snappy.parquet")
    df.to_parquet(FORMATTED_BOX_OFFICE_FOLDER  + parquet_file_name)