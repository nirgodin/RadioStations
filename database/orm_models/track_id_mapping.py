from sqlalchemy import Column, String

from database.orm_models.base_orm_model import BaseORMModel


class TrackIDMapping(BaseORMModel):
    __tablename__ = "tracks_ids_mapping"

    id = Column(String, primary_key=True, nullable=False)
    shazam_id = Column(String)
    musixmatch_id = Column(String)
    genius_id = Column(String)
    adam_id = Column(String)
