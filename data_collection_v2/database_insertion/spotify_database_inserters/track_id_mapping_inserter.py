from typing import Type, List

from genie_datastores.postgres.models import TrackIDMapping
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel

from data_collection_v2.database_insertion.spotify_database_inserters.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter


class TrackIDMappingDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    @property
    def name(self) -> str:
        return "track_ids_mapping"

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return TrackIDMapping
