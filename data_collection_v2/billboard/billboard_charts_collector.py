from datetime import datetime
from functools import partial
from typing import List

from aiohttp import ClientSession
from asyncio_pool import AioPool
from billboard import ChartData
from bs4 import BeautifulSoup
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.billboard_consts import BILLBOARD_DAILY_CHARTS_URL_FORMAT, BILLBOARD_HOT_100
from consts.datetime_consts import BILLBOARD_DATETIME_FORMAT


class BillboardChartsCollector:
    def __init__(self, session: ClientSession):
        self._session = session

        # TODO:
        #  1. Search tracks on Spotify to match them with track id
        #  2. Insert non existing tracks ids to SpotifyTrack table (incl. inserting to foreign keys)
        #  3. Insert non existing billboard tracks to BillboardTrack table
        #  4. Insert chart entries to BillboardChartEntry

    async def collect(self, dates: List[datetime]):
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(dates)) as progress_bar:
            func = partial(self._collect_single_date_charts, progress_bar)
            return await pool.map(func, dates)

    async def _collect_single_date_charts(self, progress_bar: tqdm, date: datetime) -> ChartData:
        progress_bar.update(1)
        formatted_date = date.strftime(BILLBOARD_DATETIME_FORMAT)
        url = BILLBOARD_DAILY_CHARTS_URL_FORMAT.format(name=BILLBOARD_HOT_100, date=formatted_date)

        async with self._session.get(url) as raw_response:
            chart_page_text = await raw_response.text()

        return self._create_chart_data_from_text(chart_page_text, formatted_date)

    @staticmethod
    def _create_chart_data_from_text(chart_page_text: str, formatted_date: str) -> ChartData:
        chart_data = ChartData(name=BILLBOARD_HOT_100, date=formatted_date, fetch=False)
        soup = BeautifulSoup(chart_page_text, "html.parser")
        chart_data._parsePage(soup)

        return chart_data
