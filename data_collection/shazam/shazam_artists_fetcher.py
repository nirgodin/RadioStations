from typing import List

import pandas as pd
from data_collectors import ShazamArtistsCollector, ShazamArtistsDatabaseInserter
from data_collectors.tools import AioPoolExecutor
from postgres_client import execute_query, get_database_engine, ShazamArtist
from shazamio import Shazam
from sqlalchemy import select

from consts.data_consts import ARTIST_ID
from consts.path_consts import SHAZAM_ARTISTS_IDS_PATH
from tools.data_chunks_generator import DataChunksGenerator
from utils.data_utils import extract_column_existing_values
from utils.general_utils import stringify_float


class ShazamArtistsFetcher:
    def __init__(self):
        self._db_engine = get_database_engine()
        self._data_chunks_generator = DataChunksGenerator(chunk_size=100, max_chunks_number=10)
        self._shazam_artists_collector = ShazamArtistsCollector(
            shazam=Shazam("EN"),
            pool_executor=AioPoolExecutor()
        )
        self._shazam_artists_inserter = ShazamArtistsDatabaseInserter(self._db_engine)

    async def fetch(self):
        existing_ids = await self._get_existing_artists_ids()
        await self._data_chunks_generator.execute_by_chunk(
            lst=self._get_artists_ids(),
            filtering_list=existing_ids,
            func=self._insert_single_chunk_records
        )

    @staticmethod
    def _get_artists_ids():
        artists_ids = extract_column_existing_values(path=SHAZAM_ARTISTS_IDS_PATH, column_name=ARTIST_ID)
        unique_artists_ids = {stringify_float(id_) for id_ in artists_ids if not pd.isna(id_)}

        return list(unique_artists_ids)

    async def _get_existing_artists_ids(self) -> List[str]:
        query_results = await execute_query(engine=self._db_engine, query=select(ShazamArtist.id))
        return query_results.scalars().all()

    async def _insert_single_chunk_records(self, ids: List[str]) -> None:
        artists = await self._shazam_artists_collector.collect(ids)
        valid_artists = [artist for artist in artists if isinstance(artist, dict)]

        await self._shazam_artists_inserter.insert(valid_artists)
