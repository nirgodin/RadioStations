from typing import List, Tuple

from aiohttp import ClientSession
from data_collectors.consts.eurovision_consts import EUROVISION_WIKIPEDIA_PAGE_URL_FORMAT
from genie_common.tools import AioPoolExecutor, logger


class EurovisionHTMLCollector:
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor):
        self._session = session
        self._pool_executor = pool_executor

    async def collect(self, years: List[int]) -> List[Tuple[int, str]]:
        if not years:
            logger.warn("EurovisionChartsDataCollector did not receive any valid Eurovision year to collect. Aborting")
            return []

        logger.info(f"Starting to collect eurovision data for {len(years)} years")
        return await self._query_eurovision_wikipedia_pages(years)

    async def _query_eurovision_wikipedia_pages(self, years: List[int]) -> List[Tuple[int, str]]:
        logger.info(f"Starting to collect eurovision data for {len(years)} years")
        return await self._pool_executor.run(
            iterable=years,
            func=self._query_single_year_wikipedia_page,
            expected_type=tuple
        )

    async def _query_single_year_wikipedia_page(self, year: int) -> Tuple[int, str]:
        url = EUROVISION_WIKIPEDIA_PAGE_URL_FORMAT.format(year=year)

        async with self._session.get(url) as raw_response:
            raw_response.raise_for_status()
            response = await raw_response.text()

        return year, response
