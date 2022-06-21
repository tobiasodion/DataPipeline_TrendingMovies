from pickle import FALSE
from lib.ingest.trending_movies import fetch_trending_movies_imdb
from lib.ingest.box_office_scraper import fetch_trending_movies_boxoffice
from lib.format.box_office_formatter import ingested_boxoffice_to_formatted
from lib.format.imdb_formatter import ingested_imdb_to_formatted
from lib.usage.combined_data import combine_data
from lib.index.index_data import index_data

try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
    from datetime import datetime
    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

with DAG(
    dag_id="data_pipeline_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1)
    },
    catchup=False
) as f:

   
    source_to_raw_1 = PythonOperator(
       task_id='fetch_data_from_imdb',
       python_callable=fetch_trending_movies_imdb,
       provide_context=True,
       #op_kwargs={'task_number': 'task1'}
   )

    source_to_raw_2 = PythonOperator(
       task_id='fetch_data_from_boxoffice',
       python_callable=fetch_trending_movies_boxoffice,
       provide_context=True,
       #op_kwargs={'task_number': 'task1'}
   )
   
    raw_to_formatted_1 = PythonOperator(
        task_id="format_imdb_data",
        python_callable=ingested_imdb_to_formatted,
        provide_context=True,
    )
    
    raw_to_formatted_2 = PythonOperator(
        task_id="format_boxoffice_data",
        python_callable=ingested_boxoffice_to_formatted,
        provide_context=True,
    )

    produce_usage = PythonOperator(
        task_id="produce_usage",
        python_callable=combine_data,
        provide_context=True,
    )

    index_to_elastic = PythonOperator(
        task_id="index_to_elastic",
        python_callable=index_data,
        provide_context=True,
    )

    source_to_raw_1 >> raw_to_formatted_1 >> produce_usage >> index_to_elastic
    source_to_raw_2 >> raw_to_formatted_2 >> produce_usage >> index_to_elastic