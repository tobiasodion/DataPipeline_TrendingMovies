from datetime import date
import os
import pandas as pd
import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import connections


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def index_data(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    MOVIES_RANK_RATING_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/moviesRankRating/" + current_day + "/res.snappy.parquet"
    MOVIES_RANK_REVENUE_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/moviesRankRevenue/" + current_day + "/res.snappy.parquet"
    DISTRIBUTOR_RANK_REVENUE_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/DistributorRankRevenue/" + current_day + "/res.snappy.parquet"
    DISTRIBUTOR_RANK_RATING_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/DistributorRankRating/" + current_day + "/res.snappy.parquet"
    MOVIES_CINEMA_COUNT_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/moviesCinemaCount/" + current_day + "/res.snappy.parquet"
    DISTRIBUTOR_MOVIES_COUNT_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/distributorMoviesCount/" + current_day + "/res.snappy.parquet"

    df_movies_rank_rating = pd.read_parquet(MOVIES_RANK_RATING_PATH)
    df_movies_rank_revenue = pd.read_parquet(MOVIES_RANK_REVENUE_PATH)
    df_distributor_rank_revenue = pd.read_parquet(DISTRIBUTOR_RANK_REVENUE_PATH)
    df_distributor_rank_rating = pd.read_parquet(DISTRIBUTOR_RANK_RATING_PATH)
    df_movies_cinema_count = pd.read_parquet(MOVIES_CINEMA_COUNT_PATH)
    df_distributor_movies_count = pd.read_parquet(DISTRIBUTOR_MOVIES_COUNT_PATH)

    # Check content of the DataFrame combined movies:
    #print(df_combined)

    # Index to elastic search
    es = Elasticsearch('https://localhost:9200', 
                        http_auth=('elastic', 'fnsMZKUSR0MWeejVrpvS'), 
                        use_ssl=False,
                        verify_certs=False)

    helpers.bulk(es, doc_generator(df_movies_rank_rating, 'movies-top_ranked_rating')) 
    helpers.bulk(es, doc_generator(df_movies_rank_revenue, 'movies-top_ranked_revenue'))
    helpers.bulk(es, doc_generator(df_distributor_rank_revenue, 'movies-top_ranked_distributor_revenue'))
    helpers.bulk(es, doc_generator(df_distributor_rank_rating, 'movies-top_ranked_distributor_rating'))
    helpers.bulk(es, doc_generator(df_movies_cinema_count, 'movies-cinema_count'))
    helpers.bulk(es, doc_generator(df_distributor_movies_count, 'movies-distributor_movie_count'))   

def doc_generator(df, es_index):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": es_index,
                "type": "_doc",
                "_source": document.to_dict(),
            }