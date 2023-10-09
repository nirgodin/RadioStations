from sqlalchemy import Column, String, ForeignKey, SmallInteger, Integer, Boolean, TIMESTAMP

from database.orm_models.base_orm_model import Base

# TODO: Complete relevant fields


class SpotifyTrack(Base):
    __tablename__ = "spotify_tracks"

    id = Column(String, primary_key=True, nullable=False)
    main_album_id = Column(String, ForeignKey("spotify_albums.id"), nullable=False)
    artist_id = Column(String, ForeignKey("spotify_artists.id"), nullable=False)
    name = Column(String, nullable=False)
    number = Column(SmallInteger, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    explicit = Column(Boolean, nullable=False)
    release_date = Column(TIMESTAMP, nullable=False)