from datetime import datetime

import pandas as pd
from sqlalchemy import Column, String, ForeignKey, SmallInteger, Boolean, TIMESTAMP, ARRAY

from consts.data_consts import TRACK_NUMBER, RELEASE_DATE, ALBUM_ID
from consts.shazam_consts import WRITERS
from database.orm_models.base_orm_model import BaseORMModel


class SpotifyTrack(BaseORMModel):
    __tablename__ = "spotify_tracks"

    id = Column(String, primary_key=True, nullable=False)
    album_id = Column(String)  # ForeignKey("spotify_albums.id"), nullable=False  # TODO: Add back after albums collection
    artist_id = Column(String, ForeignKey("spotify_artists.id"), nullable=False)
    explicit = Column(Boolean, nullable=False)
    name = Column(String, nullable=False)
    number = Column(SmallInteger, nullable=False)
    release_date = Column(TIMESTAMP)
    writers = Column(ARRAY(String))

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items["number"] = record_items[TRACK_NUMBER]
        record_items[RELEASE_DATE] = SpotifyTrack._pre_process_release_date(record_items[RELEASE_DATE])
        record_items[WRITERS] = None if record_items[WRITERS] is None else eval(record_items[WRITERS])

        return record_items

    @staticmethod
    def _pre_process_release_date(release_date: str) -> datetime:
        for frmt in ["%Y-%m-%d", "%Y-%m", "%Y"]:
            try:
                return datetime.strptime(release_date, frmt)
            except:
                continue
