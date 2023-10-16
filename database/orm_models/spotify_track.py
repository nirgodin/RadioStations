from datetime import datetime

import pandas as pd
from sqlalchemy import Column, String, ForeignKey, SmallInteger, Boolean, TIMESTAMP

from consts.data_consts import TRACK_NUMBER, RELEASE_DATE, ALBUM_ID
from database.orm_models.base_orm_model import BaseORMModel


# TODO: Complete relevant fields


class SpotifyTrack(BaseORMModel):
    __tablename__ = "spotify_tracks"

    id = Column(String, primary_key=True, nullable=False)
    album_id = Column(String)  # ForeignKey("spotify_albums.id"), nullable=False  # TODO: Add back after albums collection
    artist_id = Column(String, ForeignKey("spotify_artists.id"), nullable=False)
    name = Column(String, nullable=False)
    number = Column(SmallInteger, nullable=False)
    explicit = Column(Boolean, nullable=False)
    release_date = Column(TIMESTAMP)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items[ALBUM_ID] = None if pd.isna(record_items[ALBUM_ID]) else record_items[ALBUM_ID]
        record_items["number"] = record_items[TRACK_NUMBER]
        record_items[RELEASE_DATE] = SpotifyTrack._pre_process_release_date(record_items[RELEASE_DATE])

        return record_items

    @staticmethod
    def _pre_process_release_date(release_date: str) -> datetime:
        for frmt in ["%Y-%m-%d", "%Y-%m", "%Y"]:
            try:
                return datetime.strptime(release_date, frmt)
            except:
                continue
