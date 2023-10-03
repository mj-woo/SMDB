from typing import Union
from fastapi import FastAPI, Query, Depends, FastAPI, HTTPException
import requests
import json
from sqlalchemy.orm import Session
from sql_app import crud, models, schemas
from sql_app.database import SessionLocal, engine
import pandas as pd
import datetime
import ast
from fastapi.middleware.cors import CORSMiddleware
from enum import Enum
from sqlalchemy import func, Integer, cast, Date

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Configure CORS using middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],  
    allow_headers=["*"],  
    allow_credentials=True, 
)

# health check
@app.get("/health")
def health():
    return "OK"

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"

def all_movies(dataset_list: list, result: list):
    for movie in dataset_list:
        if movie['type'] =='dataverse':
            continue
        result.append(json.loads(movie['description']))
    return result

# Search bar tool: search movie; access directly from Dataverse database - to update sqlite database (sync sqlite and dataverse)
# returns a list with each movie metadata as an item in dict format
@app.get("/movies/") 
def read_movie(q: Union[str, None] = None):
    result = []
    condition = True
    start = 0
    rows = 10
    while(condition):
        if q == None:
            url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&start={start}&type=dataset"
        else:
            url = f"https://snu.dataverse.ac.kr/api/search?q={q}&subtree=movies&start={start}"
        headers = {
            "X-Dataverse-key": API_KEY
        }
        response = requests.get(url, headers = headers)

        if response.status_code == 200:
            dataset_list = response.json()["data"]["items"]
            result = all_movies(dataset_list, result)
            total = response.json()["data"]["total_count"]
            start = start + rows
            condition = start < total
        else:
            return "검색 결과 없음"
    return result

@app.post("/movies/upload/")
def create_movies(data: list[schemas.Movie], db: Session = Depends(get_db)):
    results = []
    for per_movie in data:
        db_movie = crud.get_movie_match(db, openDate=per_movie.openDate, title=per_movie.title, titleEng=per_movie.titleEng, runningTimeMinute=per_movie.runningTimeMinute)
        if db_movie:
            continue
        result = crud.insert_data_into_db(db=db, data=per_movie)
        results.append(result)
    return results

class Genre(str, Enum):
    action = "액션"
    drama = "드라마"
    comedy = "코미디"
    thriller = "스릴러"
    fantasy = "SF/판타지"
    romance = "로맨스"
    adventure = "어드벤처"
    horror = "공포"
    crime = "범죄"
    animation = "애니메이션"

# filtering tool: filter by search query, genres, and release date range
# returns a list with each movie metadata as an item in dict format
@app.get("/movies/filter/")
def filter(openyear: Union[int, None] = None, endyear: Union[int, None] = None, genres: list[Genre] = Query(None, description="List of genres to filter by"), 
           q: Union[str, None] = None, page: int = 1, per_page: int = 15,
           db: Session = Depends(get_db)):
    movies = crud.searchquery(db, genres, openyear, endyear, page, per_page, q)
    return movies

# Most Loved Movies in a list format
@app.get("/movies/mostloved/")
def mostloved(page: int = 1, per_page: int = 15, db: Session = Depends(get_db)):
    result = []
    to_return = {}
    condition = True
    # return_startidx = offset
    # return_endidx = limit
    is_last = False

    offset = 0
    limit = per_page

    start_idx = (page-1) * per_page
    end_idx = page * per_page

    # initial_start = start_idx
    # initial_end = end_idx

    while(condition):
        url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=mostloved&start={offset}&per_page={limit}"
        headers = {
            "X-Dataverse-key": API_KEY
        }
        response = requests.get(url, headers = headers)

        if response.status_code == 200:
            dataset_list = response.json()["data"]["items"]
            result = all_movies(dataset_list, result)
            total = response.json()["data"]["total_count"]
            offset += limit
            condition = offset < total
        else:
            return "검색 결과 없음"
    original_data_len = len(result)

    if end_idx >= original_data_len:
       is_last = True
    to_return['isLast'] = is_last

    paginated_final = result[start_idx : end_idx]
    to_return['data'] = crud.movies_with_id_data(paginated_final, db)
    to_return['totalCount'] = original_data_len
    return to_return

@app.post("/delete_all_records/")
def delete_records(db: Session = Depends(get_db)):
    crud.delete_all_records(db)
    return {"message": "All records deleted"}

# returns Daily Box Office Movies list
def today():
    file_path = "./kobis 8_21.csv"
    daily_boxoffice =  pd.read_csv(file_path, skiprows=6)
    daily_boxoffice.reset_index(drop=True, inplace=True)
    today_list = daily_boxoffice.iloc[:,1].tolist() 
    today_list = pd.Series(today_list).dropna().tolist()
    return today_list

# returns movies that are currently on screen 
@app.get("/movies/onscreen")
def onscreen(page: int = 1, per_page: int = 15, db: Session = Depends(get_db)):
  onscreen_list = []
  to_return = {}
#   return_startidx = offset
#   return_endidx = limit 
  offset = 0
  limit = per_page
  is_last = False
  start_idx = (page-1) * per_page
  end_idx = page * per_page

#   initial_start = start_idx
#   initial_end = end_idx

  # Box Ofice top 100 movies list
  today_list = today()
  query = db.query(models.Movie).filter(models.Movie.title.in_(today_list))
  for movie in query.all():
      movie.genre =  movie.get_list_field('genre')
      movie.directors =  movie.get_list_field('directors')
      movie.distributor =  movie.get_list_field('distributor')
      movie.posterUrl =  movie.get_list_field('posterUrl')
      movie.actors =  movie.get_list_field('actors')
      movie.producer =  movie.get_list_field('producer')
      movie.keywords =  movie.get_list_field('keywords')
      movie.vodUrl =  movie.get_list_field('vodUrl')
      movie.synopsis =  movie.get_dict_field('synopsis')
      onscreen_list.append(movie)

  # for movie in today_list:
    # url = f"https://snu.dataverse.ac.kr/api/search?q={movie}&subtree=movies&start={offset}&per_page={limit}"
    # headers = {
    #     "X-Dataverse-key": API_KEY
    # }
    # response = requests.get(url, headers = headers)

    # if response.status_code != 200:
    #   continue 
    # result = response.json()["data"]["items"]

    # if not result:
    #   print(f"'{movie}' 검색 결과 없음")
    #   continue
    # found = False
    # for item in result:
    #     if item['name'] == movie:
    #         onscreen_list.append(json.loads(item['description']) )
    #         found = True
    #         break
    # if not found:
    #     print(f"'{movie}' 검색 결과 없음")

  original_data_len = len(onscreen_list)

  paginated_final = onscreen_list[start_idx : end_idx]
  # to_return['data'] = crud.movies_with_id_data(paginated_final, db)
  to_return['data'] = paginated_final

  # check if it is the last page
  if end_idx >= original_data_len:
      is_last = True
  to_return['isLast'] = is_last

  to_return['totalCount'] = original_data_len
  return to_return

# returns movies that are will be released in the coming two years
@app.get("/movies/comingsoon")
def comingsoon(page: int = 1, per_page: int = 15, db: Session = Depends(get_db)):
  to_return = {}
  is_last = False

#   offset = 0
#   limit = per_page

  start_idx = (page-1) * per_page
  end_idx = page * per_page

#   initial_start = start_idx
#   initial_end = end_idx
  
  comingsoon_list = []
#   condition = True

  query = db.query(models.Movie).filter(models.Movie.openDate != None)
  today_date_str = datetime.date.today().strftime("%Y.%m.%d")
  query = query.filter(models.Movie.openDate > today_date_str)

  for movie in query.all():
      movie.genre =  movie.get_list_field('genre')
      movie.directors =  movie.get_list_field('directors')
      movie.distributor =  movie.get_list_field('distributor')
      movie.posterUrl =  movie.get_list_field('posterUrl')
      movie.actors =  movie.get_list_field('actors')
      movie.producer =  movie.get_list_field('producer')
      movie.keywords =  movie.get_list_field('keywords')
      movie.vodUrl =  movie.get_list_field('vodUrl')
      movie.synopsis =  movie.get_dict_field('synopsis')
      comingsoon_list.append(movie)
      

#   while(condition):
#     url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&sort=name&order=asc&per_page={offset}&start={limit}"
#     headers = {
#             "X-Dataverse-key": API_KEY
#         }
#     response = requests.get(url, headers = headers)

#     # compare release date with today's date 
#     if response.status_code == 200:
#       total = response.json()["data"]["total_count"]
#       offset += limit
#       condition = offset < total
#       dataset_list = response.json()["data"]["items"]
#       for movie in dataset_list:
#         if movie.get("description"): 
#           description = json.loads(movie['description']) 
#           opendate_str = description["openDate"] 
#           if opendate_str != '': 
#             opendate = datetime.datetime.strptime(opendate_str, "%Y.%m.%d").date()
#             if opendate > datetime.date.today(): 
#               comingsoon_list.append(description)

  original_data_len = len(comingsoon_list)
  paginated_final = comingsoon_list[start_idx : end_idx]
#   to_return['data'] = crud.movies_with_id_data(paginated_final, db)
  to_return['data'] = paginated_final

  if end_idx >= original_data_len:
       is_last = True
  to_return['isLast'] = is_last
  
  to_return['totalCount'] = original_data_len
  return to_return
    
# returns movies that are off screen
@app.get("/movies/offscreen")
def offscreen(page: int = 1, per_page: int = 15, db: Session = Depends(get_db)):
#   return_startidx = offset
#   return_endidx = limit
  to_return = {}
  is_last = False

  today_list = today()
  offscreen_list = []
#   condition = True

  # offset = 0

  start_idx = (page-1) * per_page
  end_idx = page * per_page

#   initial_start = start_idx
#   initial_end = end_idx

  query = db.query(models.Movie).filter(models.Movie.openDate != None)
  # query = query.filter(func.cast(func.substring(models.Movie.openDate, 1, 4), Integer) < datetime.date.today())
#   query = query.filter(cast(models.Movie.openDate, Date) < datetime.date.today())
  today_date_str = datetime.date.today().strftime("%Y.%m.%d")
  query = query.filter(models.Movie.openDate < today_date_str)
  query = query.filter(~models.Movie.title.in_(today_list))

  for movie in query.all():
      movie.genre =  movie.get_list_field('genre')
      movie.directors =  movie.get_list_field('directors')
      movie.distributor =  movie.get_list_field('distributor')
      movie.posterUrl =  movie.get_list_field('posterUrl')
      movie.actors =  movie.get_list_field('actors')
      movie.producer =  movie.get_list_field('producer')
      movie.keywords =  movie.get_list_field('keywords')
      movie.vodUrl =  movie.get_list_field('vodUrl')
      movie.synopsis =  movie.get_dict_field('synopsis')
      offscreen_list.append(movie)

  # while(condition):
  #   url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&sort=name&order=asc&per_page={offset}&start={per_page}"
  #   headers = {
  #           "X-Dataverse-key": API_KEY
  #       }
  #   response = requests.get(url, headers = headers)


  #   # compare release date with today's date
  #   if response.status_code == 200:
  #     dataset_list = response.json()["data"]["items"]
  #     total = response.json()["data"]["total_count"]
  #     offset+=per_page
  #     condition = offset < total
  #     for movie in dataset_list:
  #       if movie.get("description"): 
  #         description = json.loads(movie['description']) 
  #         opendate_str = description["openDate"] 
  #         if opendate_str != '': 
  #           opendate = datetime.datetime.strptime(opendate_str, "%Y.%m.%d").date()
  #           if (opendate < datetime.date.today()) and (movie not in today_list):
  #               offscreen_list.append(description)

  original_data_len = len(offscreen_list)
  paginated_final = offscreen_list[start_idx : end_idx]
  if end_idx >= original_data_len:
       is_last = True
  to_return['isLast'] = is_last

#   to_return['data'] = crud.movies_with_id_data(paginated_final, db)
  to_return['data'] = paginated_final
  to_return['totalCount'] = original_data_len
  return to_return
    
# get movies via Movie ID in database
@app.get("/movies/detail/{id}")
def read_movie(id: int, db: Session = Depends(get_db)):
    movie = db.query(models.Movie).filter(models.Movie.id == id).first()
    movie.genre =  movie.get_list_field('genre')
    movie.directors =  movie.get_list_field('directors')
    movie.distributor =  movie.get_list_field('distributor')
    movie.posterUrl =  movie.get_list_field('posterUrl')
    movie.actors =  movie.get_list_field('actors')
    movie.producer =  movie.get_list_field('producer')
    movie.keywords =  movie.get_list_field('keywords')
    movie.vodUrl =  movie.get_list_field('vodUrl')
    movie.synopsis =  movie.get_dict_field('synopsis')
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    return movie
