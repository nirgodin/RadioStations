from typing import Dict

from sqlalchemy import Column, String, ForeignKey, SmallInteger, Integer, Boolean, Float

from consts.audio_features_consts import KEY, KEY_NAMES_MAPPING, REVERSED_KEYS_MAPPING, MODE
from consts.data_consts import TIME_SIGNATURE
from database.orm_models.base_orm_model import BaseORMModel


class AudioFeatures(BaseORMModel):
    __tablename__ = "audio_features"

    id = Column(String, ForeignKey("spotify_tracks.id"), primary_key=True, nullable=False)
    acousticness = Column(Float)
    danceability = Column(Float)
    duration_ms = Column(Integer)
    energy = Column(Float)
    instrumentalness = Column(Float)
    key = Column(SmallInteger)
    liveness = Column(Float)
    loudness = Column(Float)
    mode = Column(Boolean)
    speechiness = Column(Float)
    tempo = Column(Float)
    time_signature = Column(SmallInteger)
    valence = Column(Float)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items[KEY] = REVERSED_KEYS_MAPPING.get(record_items[KEY])
        record_items[MODE] = bool(record_items[MODE])
        record_items[TIME_SIGNATURE] = int(record_items[MODE])

        return record_items
