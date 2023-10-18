from datetime import datetime
from typing import Optional

from sqlalchemy import String, Column, SmallInteger, ForeignKey, Enum, TIMESTAMP

from consts.data_consts import ID, ALBUM_ID, ALBUM_GROUP, NAME, MAIN_ALBUM, RELEASE_DATE_PRECISION, ALBUM_RELEASE_DATE, \
    RELEASE_DATE, ALBUM_TYPE, TYPE
from consts.shazam_consts import LABEL
from database.orm_models.base_orm_model import BaseORMModel
# TODO: Complete relevant fields
from models.spotify_album_type import SpotifyAlbumType


class SpotifyAlbum(BaseORMModel):
    __tablename__ = "spotify_albums"

    id = Column(String, primary_key=True, nullable=False)
    artist_id = Column(String, ForeignKey("spotify_artists.id"), nullable=False)
    group = Column(Enum(SpotifyAlbumType))
    label = Column(String)
    name = Column(String, nullable=False)
    release_date = Column(TIMESTAMP)
    total_tracks = Column(SmallInteger, nullable=False)
    type = Column(Enum(SpotifyAlbumType))

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items[ID] = record_items[ALBUM_ID]
        record_items["group"] = SpotifyAlbumType(record_items[ALBUM_GROUP]) if record_items[ALBUM_GROUP] else None
        record_items[NAME] = record_items[MAIN_ALBUM]
        record_items[RELEASE_DATE] = SpotifyAlbum._get_release_date(record_items)
        record_items[TYPE] = SpotifyAlbumType(record_items[ALBUM_TYPE]) if record_items[ALBUM_TYPE] else None
        record_items[LABEL.lower()] = record_items[LABEL]

        return record_items

    @staticmethod
    def _get_release_date(record_items: dict) -> Optional[datetime]:
        release_date_precision = record_items[RELEASE_DATE_PRECISION]
        release_date = record_items[ALBUM_RELEASE_DATE]

        if release_date is None:
            return

        if release_date_precision == "day":
            return datetime.strptime(release_date, "%Y-%m-%d")

        if release_date_precision == "month":
            return datetime.strptime(release_date, "%Y-%m")

        return datetime.strptime(release_date, "%Y")
