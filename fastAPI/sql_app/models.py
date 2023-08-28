from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .database import Base
import json

class Movie(Base):
    __tablename__ = 'movies'
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    titleEng = Column(String)
    genre = Column(String)
    synopsis = Column(String)
    openDate = Column(String)
    runningTimeMinute = Column(String)
    actors = Column(String)
    directors = Column(String)
    producer = Column(String)
    distributor = Column(String)
    keywords = Column(String)
    posterUrl = Column(String)
    vodUrl = Column(String)

    def set_list_field(self, field_name, data_list):
        current_data = self.get_list_field(field_name)
        current_data.extend(data_list)
        setattr(self, field_name, json.dumps(current_data, ensure_ascii=False))

    def set_dict_field(self, field_name, data_dict):
        current_data = self.get_dict_field(field_name)
        current_data.update(data_dict)
        setattr(self, field_name, json.dumps(current_data, ensure_ascii=False))

    def get_list_field(self, field_name):
        field_value = getattr(self, field_name)
        return json.loads(field_value) if field_value else []

    def get_dict_field(self, field_name):
        field_value = getattr(self, field_name)
        return json.loads(field_value) if field_value else {}