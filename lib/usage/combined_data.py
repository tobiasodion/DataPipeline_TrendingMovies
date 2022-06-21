from datetime import date
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def combine_data(**kwargs):
    current_day = date.today().strftime("%Y%m%d")

    FORMATTED_IMDB_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/trending_movies/" + current_day + "/"
    FORMATTED_BOX_OFFICE_PATH = DATALAKE_ROOT_FOLDER + "formatted/boxoffice/box_office_movies/" + current_day + "/"

    USAGE_OUTPUT_FOLDER_MOVIES_RANK_RATING = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/moviesRankRating/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_MOVIES_RANK_REVENUE = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/moviesRankRevenue/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_REVENUE = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/DistributorRankRevenue/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_RATING = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/DistributorRankRating/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_MOVIES_CINEMA_COUNT = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/moviesCinemaCount/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_DISTRIBUTOR_MOVIES_COUNT = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/distributorMoviesCount/" + current_day + "/"

    if not os.path.exists(USAGE_OUTPUT_FOLDER_MOVIES_RANK_RATING):
        os.makedirs(USAGE_OUTPUT_FOLDER_MOVIES_RANK_RATING)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_MOVIES_RANK_REVENUE):
        os.makedirs(USAGE_OUTPUT_FOLDER_MOVIES_RANK_REVENUE)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_REVENUE):
        os.makedirs(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_REVENUE)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_RATING):
        os.makedirs(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_RATING)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_MOVIES_CINEMA_COUNT):
        os.makedirs(USAGE_OUTPUT_FOLDER_MOVIES_CINEMA_COUNT)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_MOVIES_COUNT):
        os.makedirs(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_MOVIES_COUNT)


    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    df_trending_movies = sqlContext.read.parquet(FORMATTED_IMDB_PATH)
    df_box_office = sqlContext.read.parquet(FORMATTED_BOX_OFFICE_PATH)

    df_trending_movies.registerTempTable('df_trending_movies')
    df_box_office.registerTempTable('df_box_office')

    # Check content of the DataFrame of both datasources:
    print(df_trending_movies.show())
    print(df_box_office.show())

    #build top movies by user rating analytics dataframe
    df_movies_rank_rating = sqlContext.sql("SELECT t.original_title, t.vote_count, t.vote_average FROM df_trending_movies t ORDER by t.vote_average desc")
    # Check content of the DataFrame and save it:
    print(df_movies_rank_rating.show())
    df_movies_rank_rating.write.save(USAGE_OUTPUT_FOLDER_MOVIES_RANK_RATING + "res.snappy.parquet", mode="overwrite")

    #build top movies by revenue analytics dataframe
    df_movies_rank_revenue = sqlContext.sql("SELECT title, max(total_revenue) FROM df_box_office group by title ORDER by max(total_revenue) desc")
    # Check content of the DataFrame and save it:
    print(df_movies_rank_revenue.show())
    df_movies_rank_revenue.write.save(USAGE_OUTPUT_FOLDER_MOVIES_RANK_REVENUE + "res.snappy.parquet", mode="overwrite")

    #build df_distributor_rank_revenue analytics dataframe
    df_distributor_rank_revenue= sqlContext.sql("select distributor, grouping(distributor), avg(total_revenue) from df_box_office group by cube(distributor) order by avg(total_revenue) desc")
    # Check content of the DataFrame and save it:
    print(df_distributor_rank_revenue.show())
    df_distributor_rank_revenue.write.save(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_REVENUE + "res.snappy.parquet", mode="overwrite")

    #build df_distributor_rank_rating analytics dataframe
    df_distributor_rank_rating = sqlContext.sql("select b.distributor, avg(t.vote_average) from df_box_office b, df_trending_movies t where b.title == t.original_title group by b.distributor order by avg(vote_average) desc")
    # Check content of the DataFrame and save it:
    print(df_distributor_rank_rating.show())
    df_distributor_rank_rating.write.save(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_RANK_RATING + "res.snappy.parquet", mode="overwrite")

    #build df_movies_cinema_count analytics dataframe
    df_movies_cinema_count = sqlContext.sql("select title, max(theatre_count) from df_box_office group by title order by max(theatre_count) desc")
    # Check content of the DataFrame and save it:
    print(df_movies_cinema_count.show())
    df_movies_cinema_count.write.save(USAGE_OUTPUT_FOLDER_MOVIES_CINEMA_COUNT + "res.snappy.parquet", mode="overwrite")

    #build df_distributor_movies_count analytics dataframe
    df_distributor_movies_count = sqlContext.sql("select distributor, count(distinct(title)) from df_box_office group by distributor order by count(title) desc")
    # Check content of the DataFrame and save it:
    print(df_distributor_movies_count.show())
    df_distributor_movies_count.write.save(USAGE_OUTPUT_FOLDER_DISTRIBUTOR_MOVIES_COUNT + "res.snappy.parquet", mode="overwrite")