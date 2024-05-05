import asyncio

import numpy as np
import pandas as pd
from data_collectors.components import ComponentFactory
from genie_common.tools import SyncPoolExecutor
from genie_datastores.postgres.models import ChartEntry, Chart, SpotifyTrack, SpotifyArtist
from genie_datastores.postgres.operations import get_database_engine, read_sql
from pandas import DataFrame
from spotipyio import SpotifyClient
from sqlalchemy import select, extract

from tools.environment_manager import EnvironmentManager
from utils.file_utils import to_csv

QUERY_COLUMNS = [
    extract("year", ChartEntry.date).label("year"),
    ChartEntry.entry_metadata["Country"].label("country"),
    ChartEntry.track_id,
    SpotifyTrack.name,
    SpotifyArtist.name.label("artist_name")
]


class EurovisionSongsPopularityCollector:
    def __init__(self, client: SpotifyClient, pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._client = client
        self._pool_executor = pool_executor
        self._db_engine = get_database_engine()

    async def collect(self) -> None:
        data = await self._query_eurovision_tracks()
        popularity_data = await self._get_popularity_data(data)
        merged_data = data.merge(
            right=popularity_data,
            on="track_id",
            how="left"
        )
        to_csv(merged_data, r'data/eurovision/songs_popularity.csv')
        merged_data.to_clipboard(index=False)

    async def _query_eurovision_tracks(self) -> DataFrame:
        query = (
            select(*QUERY_COLUMNS)
            .where(ChartEntry.track_id.isnot(None))
            .where(ChartEntry.chart == Chart.EUROVISION)
            .where(ChartEntry.track_id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
        )
        return await read_sql(engine=self._db_engine, query=query)

    async def _get_popularity_data(self, data: DataFrame) -> DataFrame:
        track_ids = data["track_id"].tolist()
        tracks = await self._client.tracks.run(track_ids)
        records = self._pool_executor.run(
            iterable=tracks,
            func=self._extract_single_track_popularity,
            expected_type=dict
        )

        return pd.DataFrame.from_records(records)

    @staticmethod
    def _extract_single_track_popularity(track: dict) -> dict:
        track_id = track["id"]

        if track["available_markets"]:
            popularity = track["popularity"]
        else:
            popularity = np.nan

        return {
            "track_id": track_id,
            "popularity": popularity
        }


async def main():
    component_factory = ComponentFactory()
    spotify_session = component_factory.sessions.get_spotify_session()

    async with spotify_session as session:
        client = component_factory.tools.get_spotify_client(session)
        collector = EurovisionSongsPopularityCollector(client)
        await collector.collect()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
