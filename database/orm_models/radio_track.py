from datetime import datetime

from sqlalchemy import Column, Integer, SmallInteger, String, TIMESTAMP, ForeignKey, UniqueConstraint
from sqlalchemy import Enum

from consts.data_consts import ID, STATION, ADDED_AT
from consts.datetime_consts import SPOTIFY_DATETIME_FORMAT
from consts.playlists_consts import STATIONS
from database.orm_models.base_orm_model import BaseORMModel
from models.spotify_station import SpotifyStation

# TODO:
#  1. Add track_id fetching to all radio stations snapshots
#  2. Add release date precision to track fetching to be able to serialize to timestamp
#  3. Add album_id fetching + album details to all radio stations snapshots
#  4. Add artist_id fetching + artist details to all radio stations snapshots


class RadioTrack(BaseORMModel):
    __tablename__ = "radio_tracks"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    track_id = Column(String, ForeignKey("spotify_tracks.id"), nullable=False)
    added_at = Column(TIMESTAMP, nullable=False)
    artist_followers = Column(Integer, nullable=False)
    artist_popularity = Column(SmallInteger, nullable=False)
    popularity = Column(SmallInteger, nullable=False)
    snapshot_id = Column(String, nullable=False)
    station = Column(Enum(SpotifyStation), nullable=False)

    UniqueConstraint(track_id, added_at, station)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items["track_id"] = record_items[ID]
        record_items.pop(ID)
        record_items["station"] = SpotifyStation(STATIONS[record_items[STATION].lower()])
        record_items[ADDED_AT] = datetime.strptime(record_items[ADDED_AT], SPOTIFY_DATETIME_FORMAT)

        return record_items
