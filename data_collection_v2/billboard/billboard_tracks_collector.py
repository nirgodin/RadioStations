from datetime import datetime
from functools import partial
from typing import List, Tuple

import pandas
from aiohttp import ClientSession
from asyncio_pool import AioPool
from billboard import ChartData, ChartEntry
from spotipyio.logic.collectors.search_collectors.search_item import SearchItem
from spotipyio.logic.collectors.search_collectors.spotify_search_type import SpotifySearchType
from spotipyio.logic.spotify_client import SpotifyClient
from spotipyio.utils.spotify_utils import extract_first_search_result
from tqdm import tqdm

from consts.data_consts import TRACK
from consts.datetime_consts import BILLBOARD_DATETIME_FORMAT
from data_collection_v2.billboard.chart_entry_data import ChartEntryData


class BillboardTracksCollector:
    def __init__(self, session: ClientSession, spotify_client: SpotifyClient):
        self._session = session
        self._spotify_client = spotify_client

    async def collect(self, charts: List[ChartData]):
        chart_entries = self._get_flattened_chart_entries(charts)[:10]
        pool = AioPool(3)

        with tqdm(total=len(chart_entries)) as progress_bar:
            func = partial(self._collect_single, progress_bar)
            results = await pool.map(func, chart_entries)

        return [result for result in results if isinstance(result, ChartEntryData)]

    async def _collect_single(self, progress_bar: tqdm, entry_data: ChartEntryData) -> ChartEntryData:
        progress_bar.update(1)
        search_item = SearchItem(
            search_types=[SpotifySearchType.TRACK],
            artist=entry_data.entry.artist,
            track=entry_data.entry.title
        )
        search_result = await self._spotify_client.search.collect_single(search_item)
        raw_track = extract_first_search_result(search_result)
        entry_data.track = {TRACK: raw_track} if raw_track else None

        return entry_data

    @staticmethod
    def _get_flattened_chart_entries(charts: List[ChartData]) -> List[ChartEntryData]:
        entries = []

        for chart in charts:
            for entry in chart.entries:
                entry_data = ChartEntryData(
                    entry=entry,
                    date=datetime.strptime(chart.date, BILLBOARD_DATETIME_FORMAT),
                    chart=chart.name
                )
                entries.append(entry_data)

        return entries
