from sqlalchemy import Column, String, ARRAY

from database.orm_models.base_orm_model import Base

# TODO: Complete relevant fields


class SpotifyArtist(Base):
    __tablename__ = "spotify_artists"

    id = Column(String, primary_key=True, nullable=False)
    name = Column(String, nullable=False)
    genres = Column(ARRAY(String))
