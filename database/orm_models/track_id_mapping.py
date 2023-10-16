from sqlalchemy import Column, String, ForeignKey

from database.orm_models.base_orm_model import BaseORMModel


class TrackIDMapping(BaseORMModel):
    __tablename__ = "track_ids_mapping"

    id = Column(String, ForeignKey("spotify_tracks.id"), primary_key=True, nullable=False)
    adam_id = Column(String)
    apple_music_id = Column(String)
    genius_id = Column(String)
    musixmatch_id = Column(String)
    shazam_id = Column(String)
