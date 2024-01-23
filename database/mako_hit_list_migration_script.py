import asyncio
from datetime import date, datetime
from functools import partial
from typing import List, Optional, Set

from async_lru import alru_cache
from data_collectors.components import ComponentFactory
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.models import ChartEntry, Chart, SpotifyTrack
from genie_datastores.postgres.operations import get_database_engine, execute_query, insert_records
from pandas import DataFrame
from spotipyio import SearchItem, SearchItemMetadata, SpotifySearchType, SpotifyClient
from spotipyio.logic.authentication.spotify_session import SpotifySession
from spotipyio.utils import extract_first_search_result
from sqlalchemy import select
from tqdm import tqdm

from consts.data_consts import SCRAPED_AT, NAME, ARTIST_NAME, ID, TRACK
from consts.datetime_consts import DATETIME_FORMAT
from consts.mako_hit_list_consts import CURRENT_RANK, OVERALL
from consts.path_consts import MAKO_HIT_LIST_DIR_PATH
from data_processing.data_merger import DataMerger
from tools.environment_manager import EnvironmentManager


class MakoHitListMigrator:
    def __init__(self, spotify_client: SpotifyClient, spotify_insertions_manager: SpotifyInsertionsManager):
        self._spotify_client = spotify_client
        self._spotify_insertions_manager = spotify_insertions_manager
        self._db_engine = get_database_engine()
        self._pool_executor = AioPoolExecutor()
        self._data_merger = DataMerger(drop_duplicates_on=[CURRENT_RANK, ARTIST_NAME, NAME, SCRAPED_AT])

    async def migrate(self):
        print("Starting to merge and extract all overall data")
        data = self._data_merger.merge(dir_path=MAKO_HIT_LIST_DIR_PATH)
        overall_data = data[data["chart_name"] == OVERALL]
        records = await self._create_records(overall_data)
        await self._insert_missing_spotify_tracks(records)
        await self._insert_charts_entries(records)

    async def _create_records(self, data: DataFrame) -> List[List[ChartEntry]]:
        unique_dates = data[SCRAPED_AT].unique().tolist()
        records = []

        with tqdm(total=len(unique_dates)) as progress_bar:
            for date_ in unique_dates:
                print(f"Starting to create records for data `{date_}`")
                date_records = await self._generate_single_date_records(data, date_)
                non_duplicate_records = self._filter_duplicate_records(date_records)
                records.append(non_duplicate_records)
                progress_bar.update(1)

        return records

    async def _generate_single_date_records(self, data: DataFrame, date_: str) -> List[ChartEntry]:
        date_data = data[data[SCRAPED_AT] == date_].reset_index(drop=True)
        date_ = self._parse_date(date_)
        date_records = []

        for i, row in date_data.iterrows():
            key = f"{row[ARTIST_NAME]} - {row[NAME]}"
            track_id = await self._get_track_id(key)

            if track_id is not None:
                record = ChartEntry(
                    track_id=track_id,
                    chart=Chart.MAKO_WEEKLY_HIT_LIST,
                    date=date_,
                    position=i + 1,
                    key=key
                )
                date_records.append(record)

        return date_records

    @staticmethod
    def _parse_date(raw_date: str) -> date:
        serialized_date = datetime.strptime(raw_date, DATETIME_FORMAT)
        return serialized_date.date()

    @alru_cache(maxsize=4000)
    async def _get_track_id(self, key: str) -> Optional[str]:
        search_item = SearchItem(
            text=key,
            metadata=SearchItemMetadata(
                search_types=[SpotifySearchType.TRACK],
                quote=False
            )
        )
        search_result = await self._spotify_client.search.run_single(search_item)
        track = extract_first_search_result(search_result)

        if track is not None:
            return track[ID]

    @staticmethod
    def _filter_duplicate_records(records: List[ChartEntry]) -> List[ChartEntry]:
        unique_records = []
        unique_identifiers = []

        for record in records:
            record_identifier = (record.track_id, record.chart, record.date)

            if record_identifier not in unique_identifiers:
                unique_records.append(record)
                unique_identifiers.append(record_identifier)

        return unique_records

    async def _insert_missing_spotify_tracks(self, records: List[List[ChartEntry]]) -> None:
        print("Starting to insert missing spotify tracks to database before inserting charts entries")
        tracks_ids = self._get_unique_tracks_ids(records)
        missing_tracks_ids = await self._get_missing_tracks_ids(tracks_ids)
        print(f"Found {len(missing_tracks_ids)} missing tracks. Fetching")

        if missing_tracks_ids:
            tracks = await self._spotify_client.tracks.run(missing_tracks_ids)
            formalized_tracks = [{TRACK: track} for track in tracks]

            await self._spotify_insertions_manager.insert(formalized_tracks)

    @staticmethod
    def _get_unique_tracks_ids(records: List[List[ChartEntry]]) -> Set[str]:
        tracks_ids = set()

        for chart_date_records in records:
            for record in chart_date_records:
                if record.track_id not in tracks_ids:
                    tracks_ids.add(record.track_id)

        return tracks_ids

    async def _get_missing_tracks_ids(self, tracks_ids: Set[str]) -> List[str]:
        query_result = await execute_query(query=select(SpotifyTrack.id), engine=self._db_engine)
        existing_ids = query_result.scalars().all()

        return [track_id for track_id in tracks_ids if track_id not in existing_ids]

    async def _insert_charts_entries(self, records: List[List[ChartEntry]]) -> None:
        print("Starting to insert spotify charts entries")
        await self._pool_executor.run(
            iterable=records,
            func=partial(insert_records, self._db_engine),
            expected_type=type(None)
        )


async def run_mako_hit_list_migration() -> None:
    async with SpotifySession() as session:
        spotify_client = SpotifyClient.create(session)
        factory = ComponentFactory()
        insertions_manager = factory.spotify.inserters.spotify.get_insertions_manager(spotify_client)
        migrator = MakoHitListMigrator(spotify_client, insertions_manager)

        await migrator.migrate()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_mako_hit_list_migration())
