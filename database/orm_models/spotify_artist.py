import pandas as pd
from sqlalchemy import Column, String, ARRAY, Enum

from consts.data_consts import GENRES, ID, ARTIST_ID, NAME, ARTIST_NAME
from consts.openai_consts import ARTIST_GENDER
from database.orm_models.base_orm_model import BaseORMModel
# TODO: Complete relevant fields
from models.gender import Gender


class SpotifyArtist(BaseORMModel):
    __tablename__ = "spotify_artists"

    id = Column(String, primary_key=True, nullable=False)
    name = Column(String, nullable=False)
    gender = Column(Enum(Gender))
    genres = Column(ARRAY(String))

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items[GENRES] = eval(record_items[GENRES])
        record_items[NAME] = record_items[ARTIST_NAME]
        record_items[ID] = record_items[ARTIST_ID]
        raw_gender = record_items[ARTIST_GENDER]
        record_items["gender"] = Gender(raw_gender) if not pd.isna(raw_gender) else None

        return record_items
