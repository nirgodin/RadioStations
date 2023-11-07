from typing import List, Optional, Type

from postgres_client.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from postgres_client.models.orm.spotify.spotify_album import SpotifyAlbum
from postgres_client.models.orm.spotify.spotify_track import SpotifyTrack
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import TRACK, ALBUM
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter


class TracksDatabaseInserter(BaseDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine):
        super().__init__(db_engine)

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyTrack
