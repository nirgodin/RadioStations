from typing import List, Type

from postgres_client.models.orm.spotify.audio_features import AudioFeatures
from postgres_client.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from postgres_client.utils.dict_utils import safe_nested_get
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import TRACK, ID
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter


class AudioFeaturesDatabaseInserter(BaseDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        ids = [safe_nested_get(track, [TRACK, ID]) for track in tracks]
        return await self._spotify_client.audio_features.collect(ids)

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return AudioFeatures
