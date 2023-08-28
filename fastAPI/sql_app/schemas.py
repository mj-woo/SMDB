from pydantic import BaseModel

class MovieBase(BaseModel):
    title: str
    directors: list[str]

class Movie(MovieBase):
    titleEng: str
    genre: list[str]
    synopsis: dict
    openDate: str
    runningTimeMinute: str
    actors: list[str]
    producer: list[str]
    distributor: list[str]
    keywords: list[str]
    posterUrl: list[str]
    vodUrl: list[list[str]]

class MovieCreate(Movie):
    pass