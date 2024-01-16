import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Dict, List, Tuple, Set

import pandas as pd
from async_lru import alru_cache
from data_collectors.components import ComponentFactory
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.models import Chart, ChartEntry, SpotifyTrack
from genie_datastores.postgres.operations import get_database_engine, execute_query, insert_records
from pandas import DataFrame, Series
from spotipyio import SpotifyClient, SearchItem, SearchItemMetadata, SpotifySearchType
from spotipyio.logic.authentication.spotify_session import SpotifySession
from spotipyio.utils import extract_first_search_result
from sqlalchemy import select
from tqdm import tqdm

from consts.data_consts import STATION, ADDED_AT, ID, ARTIST_NAME, NAME, TRACK
from consts.datetime_consts import SPOTIFY_DATETIME_FORMAT
from consts.path_consts import SPOTIFY_CHARTS_OUTPUT_PATH
from consts.playlists_consts import SPOTIFY_TOP_50_ISRAEL_DAILY, SPOTIFY_TOP_50_GLOBAL_DAILY
from consts.spotify_consts import RADIO_SNAPSHOTS_DUPLICATE_COLUMNS
from tools.environment_manager import EnvironmentManager


@dataclass
class DateChart:
    date: datetime
    chart: Chart
    data: DataFrame


class SpotifyTopTracksMigrator:
    def __init__(self, spotify_client: SpotifyClient, spotify_insertions_manager: SpotifyInsertionsManager):
        self._spotify_client = spotify_client
        self._spotify_insertions_manager = spotify_insertions_manager
        self._db_engine = get_database_engine()
        self._pool_executor = AioPoolExecutor()

    async def migrate(self):
        data = self._get_data()
        grouped_data_by_date = self._group_data_by_date_and_chart(data)
        existing_charts_dates = await self._get_existing_charts_dates()
        filtered_grouped_data = self._filter_existing_charts_dates(grouped_data_by_date, existing_charts_dates)
        records = await self._create_records(filtered_grouped_data)
        await self._insert_missing_spotify_tracks(records)
        await self._insert_charts_entries(records)

    @staticmethod
    def _get_data() -> DataFrame:
        print('Loading raw data')
        data = pd.read_csv(SPOTIFY_CHARTS_OUTPUT_PATH)
        relevant_charts_data = data[data[STATION].isin([SPOTIFY_TOP_50_ISRAEL_DAILY, SPOTIFY_TOP_50_GLOBAL_DAILY])]

        return relevant_charts_data.drop_duplicates(subset=RADIO_SNAPSHOTS_DUPLICATE_COLUMNS)

    def _group_data_by_date_and_chart(self, data: DataFrame) -> List[DateChart]:
        print('Grouping data by date and chart')
        data_by_date = []
        unique_dates = data[ADDED_AT].unique().tolist()

        with tqdm(total=len(unique_dates)) as progress_bar:
            for date in unique_dates:
                for station, chart in self._station_chart_mapping.items():
                    date_data = data[(data[ADDED_AT] == date) & (data[STATION] == station)]

                    if not date_data.empty:
                        date_chart = DateChart(
                            date=datetime.strptime(date, SPOTIFY_DATETIME_FORMAT).date(),
                            chart=chart,
                            data=date_data.reset_index(drop=True)
                        )
                        data_by_date.append(date_chart)

                progress_bar.update(1)

        return data_by_date

    async def _get_existing_charts_dates(self) -> List[Tuple[Chart, datetime]]:
        query = (
            select(ChartEntry.chart, ChartEntry.date)
            .where(ChartEntry.chart.in_([Chart.SPOTIFY_DAILY_ISRAELI, Chart.SPOTIFY_DAILY_INTERNATIONAL]))
            .distinct(ChartEntry.chart, ChartEntry.date)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        existing_charts_dates = query_result.all()

        return [(row.chart, row.date.date()) for row in existing_charts_dates]

    @staticmethod
    def _filter_existing_charts_dates(grouped_data_by_date: List[DateChart],
                                      existing_charts_dates: List[Tuple[Chart, datetime]]) -> List[DateChart]:
        filtered_charts = []

        for date_chart in grouped_data_by_date:
            if (date_chart.chart, date_chart.date) not in existing_charts_dates:
                filtered_charts.append(date_chart)

        return filtered_charts

    async def _create_records(self, grouped_data_by_date: List[DateChart]) -> List[List[ChartEntry]]:
        records = []

        for date_chart in grouped_data_by_date:
            print(f"Starting to convert entries for date {date_chart.date.strftime(SPOTIFY_DATETIME_FORMAT)} and chart {date_chart.chart.value}")
            date_chart_records = await self._to_chart_entries(date_chart)
            non_duplicate_records = self._filter_duplicate_records(date_chart_records)
            records.append(non_duplicate_records)

        return records

    async def _to_chart_entries(self, date_chart: DateChart) -> List[ChartEntry]:
        playlist_id = self._chart_playlist_ids_mapping[date_chart.chart]
        func = partial(self._convert_single_row_to_chart_entry, date_chart.chart, date_chart.date, playlist_id)

        return await self._pool_executor.run(
            iterable=list(date_chart.data.iterrows()),
            func=func,
            expected_type=ChartEntry
        )

    async def _convert_single_row_to_chart_entry(self, chart: Chart, date: datetime, playlist_id: str, index_and_row: Tuple[int, Series]) -> ChartEntry:
        index, row = index_and_row
        key = f"{row[ARTIST_NAME]} - {row[NAME]}"
        if pd.isna(row[ID]):
            track_id = await self._get_missing_track_id(key)
        else:
            track_id = row[ID]

        if track_id is not None:
            return ChartEntry(
                track_id=track_id,
                chart=chart,
                date=date,
                key=key,
                position=index + 1,
                comment=playlist_id
            )

    @alru_cache(maxsize=1000)
    async def _get_missing_track_id(self, key: str) -> str:
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

    @property
    def _station_chart_mapping(self) -> Dict[str, Chart]:
        return {
            'spotify_top_50_israel_daily': Chart.SPOTIFY_DAILY_ISRAELI,
            'spotify_top_50_global_daily': Chart.SPOTIFY_DAILY_INTERNATIONAL
        }

    @property
    def _chart_playlist_ids_mapping(self) -> Dict[Chart, str]:
        return {
            Chart.SPOTIFY_DAILY_ISRAELI: "37i9dQZEVXbJ6IpvItkve3",
            Chart.SPOTIFY_DAILY_INTERNATIONAL: "37i9dQZEVXbMDoHDwVN2tF"
        }

    async def _insert_charts_entries(self, records: List[List[ChartEntry]]) -> None:
        print("Starting to insert spotify charts entries")
        await self._pool_executor.run(
            iterable=records,
            func=partial(insert_records, self._db_engine),
            expected_type=type(None)
        )


async def run_spotify_top_tracks_migration() -> None:
    async with SpotifySession() as session:
        spotify_client = SpotifyClient.create(session)
        factory = ComponentFactory()
        insertions_manager = factory.spotify.inserters.spotify.get_insertions_manager(spotify_client)
        migrator = SpotifyTopTracksMigrator(spotify_client, insertions_manager)

        await migrator.migrate()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_spotify_top_tracks_migration())
