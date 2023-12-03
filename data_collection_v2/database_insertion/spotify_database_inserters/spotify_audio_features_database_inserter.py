from typing import List, Type

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import AudioFeatures
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import TRACK, ID
from data_collection_v2.database_insertion.spotify_database_inserters.base_spotify_database_inserter import BaseSpotifyDatabaseInserter


class SpotifyAudioFeaturesDatabaseInserter(BaseSpotifyDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        ids = {safe_nested_get(track, [TRACK, ID]) for track in tracks}
        return await self._spotify_client.audio_features.collect(list(ids))

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return AudioFeatures

    @property
    def name(self) -> str:
        return "audio_features"
