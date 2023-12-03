from typing import List, Type

from genie_datastores.postgres.models import SpotifyTrack
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel

from consts.data_consts import TRACKS
from data_collection_v2.database_insertion.spotify_database_inserters.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter


class SpotifyTracksDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyTrack

    @property
    def name(self) -> str:
        return TRACKS
