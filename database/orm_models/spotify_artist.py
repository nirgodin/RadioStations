from datetime import datetime
from typing import Union, Optional

import pandas as pd
from sqlalchemy import Column, String, ARRAY, Enum, TIMESTAMP, Boolean

from consts.data_consts import GENRES, ID, ARTIST_ID, NAME, ARTIST_NAME, IS_ISRAELI
from consts.datetime_consts import DATETIME_FORMAT
from consts.gender_consts import SOURCE
from consts.openai_consts import ARTIST_GENDER
from consts.wikipedia_consts import BIRTH_DATE, DEATH_DATE
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
        gender_source = record_items[SOURCE]
        record_items["gender_source"] = DataSource(gender_source) if not pd.isna(gender_source) else None
        record_items[BIRTH_DATE] = SpotifyArtist._parse_date(record_items[BIRTH_DATE])
        record_items[DEATH_DATE] = SpotifyArtist._parse_date(record_items[DEATH_DATE])
        record_items[IS_ISRAELI] = None if pd.isna(record_items[IS_ISRAELI]) else record_items[IS_ISRAELI]

        return record_items

    @staticmethod
    def _parse_date(raw_date: Union[str, float]) -> Optional[datetime]:
        if pd.isna(raw_date):
            return None

        return datetime.strptime(raw_date, DATETIME_FORMAT)
