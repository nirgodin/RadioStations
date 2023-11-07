from sqlalchemy import String, Column, Enum, SmallInteger

from database.orm_models.base_orm_model import BaseORMModel
from database.orm_models.shazam.shazam_location import ShazamLocation


class ShazamTopTrack(BaseORMModel):
    __tablename__ = "shazam_top_tracks"

    id = Column(String, primary_key=True, nullable=False)
    location = Column(Enum(ShazamLocation), nullable=False)
    rank = Column(SmallInteger, nullable=False)