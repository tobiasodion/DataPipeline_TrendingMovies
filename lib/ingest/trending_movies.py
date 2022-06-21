import json
import requests
from requests.structures import CaseInsensitiveDict
import configparser
from datetime import date
import os

#folder constants
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def fetch_trending_movies_imdb(**kwargs):
  #Get data from themoviesdb API
  bearer_token = ''
  base_url = 'https://api.themoviedb.org/3'
  endpoint = '/trending/movie/week'
  url = base_url + endpoint

  headers = CaseInsensitiveDict()
  headers["Accept"] = "application/json"
  headers["Authorization"] = 'Bearer ' + bearer_token

  resp = requests.get(url, headers=headers)
  body = resp.json()
  movies = body['results']

  #Insert data to datalake
  current_day = date.today().strftime("%Y%m%d")
  TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/trending_movies/" + current_day + "/"
  if not os.path.exists(TARGET_PATH):
    os.makedirs(TARGET_PATH)
  print("Writing here: ", TARGET_PATH)
  f = open(TARGET_PATH + "imdb.json", "w+")
  f.write(json.dumps(movies, indent=4))