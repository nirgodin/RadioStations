from sqlalchemy import String, Column, SmallInteger

from database.orm_models.base_orm_model import Base

# TODO: Complete relevant fields


class SpotifyAlbum(Base):
    __tablename__ = "spotify_albums"

    id = Column(String, primary_key=True, nullable=False)
    number_of_tracks = Column(SmallInteger, nullable=False)
