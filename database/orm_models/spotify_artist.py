import pandas as pd
from sqlalchemy import Column, String, ARRAY, Enum, TIMESTAMP, Boolean

from consts.data_consts import GENRES, ID, ARTIST_ID, NAME, ARTIST_NAME
from consts.openai_consts import ARTIST_GENDER
from database.orm_models.base_orm_model import BaseORMModel
from models.data_source import DataSource
from models.gender import Gender
# TODO: Complete relevant fields


class SpotifyArtist(BaseORMModel):
    __tablename__ = "spotify_artists"

    id = Column(String, primary_key=True, nullable=False)
    name = Column(String, nullable=False)
    birth_date = Column(TIMESTAMP)
    death_date = Column(TIMESTAMP)
    facebook_name = Column(String)
    gender = Column(Enum(Gender))
    gender_source = Column(Enum(DataSource))
    genres = Column(ARRAY(String))
    instagram_name = Column(String)
    is_israeli = Column(Boolean)
    is_lgbtq = Column(Boolean)
    primary_genre = Column(String)
    twitter_name = Column(String)
    wikipedia_language = Column(String)
    wikipedia_name = Column(String)

    # TODO: Add logic to separate names from url
    # TODO: What to do with other UI fields?

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items[GENRES] = eval(record_items[GENRES])
        record_items[NAME] = record_items[ARTIST_NAME]
        record_items[ID] = record_items[ARTIST_ID]
        raw_gender = record_items[ARTIST_GENDER]
        record_items["gender"] = Gender(raw_gender) if not pd.isna(raw_gender) else None

        return record_items
