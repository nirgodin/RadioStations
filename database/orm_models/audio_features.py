from sqlalchemy import Column, String, ForeignKey, SmallInteger, Integer, Boolean, Float

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
