import asyncio
from copy import deepcopy
from typing import List, Tuple, Optional

import pandas as pd
from data_collectors.components import ComponentFactory
from genie_common.tools import logger
from genie_common.utils import sub_between_two_characters
from pandas import DataFrame

from data_collection.eurovision.eurovision_html_collector import EurovisionHTMLCollector
from utils.file_utils import to_csv

EUROVISION_TABLE_CONTEST_ID_COLUMNS = [
    "Country",
    "Broadcaster",
    "Artist",
    "Song",
    "Language",
    "Songwriter(s)"
]


class EurovisionLanguageCollector:
    def __init__(self, html_collector: EurovisionHTMLCollector):
        self._html_collector = html_collector

    async def collect(self, years: List[int]) -> None:
        years_to_wiki_pages = await self._html_collector.collect(years)
        tracks_languages = self._extract_tracks_languages(years_to_wiki_pages)

        to_csv(tracks_languages, r'data\eurovision\tracks_languages.csv')
        tracks_languages.to_clipboard(index=False)

    def _extract_tracks_languages(self, years_to_wiki_pages: List[Tuple[int, str]]) -> DataFrame:
        tracks_details = []

        for year, page in years_to_wiki_pages:
            year_results = self._get_single_year_tracks_details(year, page)

            if year_results is not None:
                tracks_details.append(year_results)

        return pd.concat(tracks_details)

    def _get_single_year_tracks_details(self, year: int, page: str) -> Optional[DataFrame]:
        logger.info(f"Converting year {year} data to charts entries")
        table_details = self._extract_participants_table(page)

        if table_details is None:
            logger.warn(f"Could not extract eurovision charts data entries from wikipedia. Skipping year {year} data")
        else:
            return self._pre_process_data(table_details, year)

    def _extract_participants_table(self, page: str) -> Optional[DataFrame]:
        page_tables = pd.read_html(page)
        relevant_tables = []

        for table in page_tables:
            if self._is_relevant_table(table):
                relevant_tables.append(table)

        if relevant_tables:
            return relevant_tables[0]

    @staticmethod
    def _is_relevant_table(table: DataFrame) -> bool:
        formatted_columns = []

        for column in table.columns:
            if isinstance(column, str):
                formatted_column = sub_between_two_characters(r"\[", r"\]", "", column)
                formatted_column = formatted_column.replace('Language(s)', "Language")
            else:
                formatted_column = deepcopy(column)

            formatted_columns.append(formatted_column)

        table.columns = formatted_columns
        return all(col in table.columns for col in EUROVISION_TABLE_CONTEST_ID_COLUMNS)

    def _pre_process_data(self, data: DataFrame, year: int) -> DataFrame:
        data["Country"] = data["Country"].apply(lambda x: x.replace("‡", "").strip())
        data["Song"] = data["Song"].apply(lambda x: x.strip('"'))
        data["year"] = year

        if "Ref." in data.columns:
            data.drop("Ref.", axis=1, inplace=True)

        return self._split_languages(data)

    @staticmethod
    def _split_languages(data: DataFrame) -> DataFrame:
        languages_data = data["Language"].str.split(",", expand=True)
        languages_data.fillna("", inplace=True)
        languages_data = languages_data.applymap(lambda x: x.strip())
        columns = [f"language_{i+1}" for i in range(len(languages_data.columns))]
        languages_data.columns = columns

        return pd.concat([data, languages_data], axis=1)


async def main():
    factory = ComponentFactory()
    async with factory.sessions.get_client_session() as session:
        html_collector = EurovisionHTMLCollector(session, factory.tools.get_pool_executor())
        collector = EurovisionLanguageCollector(html_collector)

        await collector.collect([2004])  #list(range(1957, 2024)))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
