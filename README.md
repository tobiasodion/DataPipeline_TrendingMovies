# Trending Movies Data Pipeline

### Overview

![Screenshot 2024-05-13 at 15 50 38](https://github.com/tobiasodion/DataPipeline_TrendingMovies/assets/32149693/f1c782f6-1185-4a37-8a12-fb60967add51)

This project involved the development of a data pipeline for trending movies using Airflow and Python. It involved the following:

- The data pipeline ingested trending movies' and distributors' data from IMDb and Box Office.
- The ingested data was cleansed, formatted, processed, and indexed on elastic search. 
- Finally, A dashboard was created from the enriched data using Kibana analytics.

Watch Demo [here](https://drive.google.com/file/d/1LgrX-VnH5w4uabVP6PhhFpUpzUs8k93l/view?usp=sharing)

### Tools & Libraries

The tools and libraries used in this project included:
- Airflow for automating the pipeline
- Selenium for data ingestion through web scraping
- Pandas for data cleansing and formatting
- Pyspark for data processing
- Elastic search and kibana.

### Dashboards
![Screenshot 2024-05-13 at 15 51 05](https://github.com/tobiasodion/DataPipeline_TrendingMovies/assets/32149693/dcfb31b4-a966-4c6c-b073-fe982ece22df)

Total revenue of all the trending movies, Top 5 trending movies by user rating, and distributors' revenue share

![Screenshot 2024-05-13 at 15 51 24](https://github.com/tobiasodion/DataPipeline_TrendingMovies/assets/32149693/e5264e6f-52e4-4ce6-9d11-38a6b29b826b)

Top 5 distributors by revenue
