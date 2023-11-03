from typing import List, Optional, Type

from postgres_client.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from postgres_client.models.orm.spotify.spotify_album import SpotifyAlbum
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import TRACK, ALBUM
from data_collection_v2.base_database_inserter import BaseDatabaseInserter
from tools.logging import logger


class AlbumsDatabaseInserter(BaseDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine):
        super().__init__(db_engine)

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        albums = []

        for track in tracks:
            album = self._extract_single_album(track)

            if album is not None:
                albums.append(album)

        return albums

    @staticmethod
    def _extract_single_album(track: dict) -> Optional[dict]:
        inner_track = track.get(TRACK, {})
        if inner_track is None:
            return

        return inner_track.get(ALBUM)

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyAlbum
