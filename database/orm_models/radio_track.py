from datetime import datetime

from sqlalchemy import Column, Integer, SmallInteger, String, TIMESTAMP

from database.orm_models.base_orm_model import Base

# TODO:
#  1. Add track_id fetching to all radio stations snapshots
#  2. Add release date precision to track fetching to be able to serialize to timestamp
#  3. Add album_id fetching + album details to all radio stations snapshots
#  4. Add artist_id fetching + artist details to all radio stations snapshots


class RadioTrack(Base):
    __tablename__ = "radio_tracks"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    popularity = Column(SmallInteger, nullable=False)
    added_at = Column(TIMESTAMP, nullable=False)
    artist_popularity = Column(SmallInteger, nullable=False)
    artist_followers = Column(Integer, nullable=False)
    station = Column(String, nullable=False)
    snapshot_id = Column(String, nullable=False)
    created_date = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
