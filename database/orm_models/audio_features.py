from typing import Dict, Optional

from sqlalchemy import Column, String, ForeignKey, SmallInteger, Integer, Boolean, Float

from consts.audio_features_consts import KEY, KEY_NAMES_MAPPING, REVERSED_KEYS_MAPPING, MODE, TEMPO, ACOUSTICNESS, \
    DANCEABILITY, ENERGY, INSTRUMENTALNESS, LIVENESS, SPEECHINESS, VALENCE
from consts.data_consts import TIME_SIGNATURE
from database.orm_models.base_orm_model import BaseORMModel

MULTIPLY_COLUMNS = [
    ACOUSTICNESS,
    DANCEABILITY,
    ENERGY,
    INSTRUMENTALNESS,
    LIVENESS,
    SPEECHINESS,
    VALENCE
]


class AudioFeatures(BaseORMModel):
    __tablename__ = "audio_features"

    id = Column(String, ForeignKey("spotify_tracks.id"), primary_key=True, nullable=False)
    acousticness = Column(SmallInteger)
    danceability = Column(SmallInteger)
    duration_ms = Column(Integer)
    energy = Column(SmallInteger)
    instrumentalness = Column(SmallInteger)
    key = Column(SmallInteger)
    liveness = Column(SmallInteger)
    loudness = Column(Float)
    mode = Column(Boolean)
    speechiness = Column(SmallInteger)
    tempo = Column(SmallInteger)
    time_signature = Column(SmallInteger)
    valence = Column(SmallInteger)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items[KEY] = REVERSED_KEYS_MAPPING.get(record_items[KEY])
        record_items[MODE] = bool(record_items[MODE])
        record_items[TIME_SIGNATURE] = int(record_items[MODE])
        record_items[TEMPO] = AudioFeatures._safe_round(record_items[TEMPO])

        for col in MULTIPLY_COLUMNS:
            record_items[col] = AudioFeatures._safe_multiply_round(record_items[col])

        return record_items

    @staticmethod
    def _safe_multiply_round(value: Optional[float]) -> Optional[int]:
        if value is not None:
            return round(value * 100)

    @staticmethod
    def _safe_round(value: Optional[float]) -> Optional[int]:
        if value is not None:
            return round(value)
