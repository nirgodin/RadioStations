import asyncio
import os.path
import re
from datetime import datetime
from functools import partial
from typing import List, Dict, Tuple

import pandas as pd
from asyncio_pool import AioPool
from tqdm import tqdm
from wikipediaapi import Wikipedia

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ARTIST_NAME, ARTIST_POPULARITY
from consts.path_consts import WIKIPEDIA_AGE_OUTPUT_PATH
from tools.data_chunks_generator import DataChunksGenerator
from utils.callable_utils import run_async
from utils.data_utils import read_merged_data
from utils.datetime_utils import DATETIME_FORMAT
from utils.file_utils import append_to_csv
from utils.regex_utils import search_between_two_characters

WIKIPEDIA_DATETIME_FORMATS = [
    '%B %d %Y',
    '%d %B %Y'
]
BIRTH_DATE = 'birth_date'
DEATH_DATE = 'death_date'


class WikipediaAgeCollector:
    def __init__(self):
        self._wikipedia = Wikipedia('en')
        self._punctuation_regex = re.compile(r'[^A-Za-z0-9]+')
        self._data_chunks_generator = DataChunksGenerator()

    async def collect(self):
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=self._get_contender_artists(),
            filtering_list=self._get_existing_artists()
        )

        for chunk in chunks:
            await self._collect_single_chunk(chunk)

    @staticmethod
    def _get_contender_artists() -> List[str]:
        data = read_merged_data()
        data.dropna(subset=[ARTIST_NAME], inplace=True)
        data.drop_duplicates(subset=[ARTIST_NAME], inplace=True)
        data.sort_values(by=[ARTIST_POPULARITY], ascending=False, inplace=True)

        return data[ARTIST_NAME].tolist()

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if not os.path.exists(WIKIPEDIA_AGE_OUTPUT_PATH):
            return []

        data = pd.read_csv(WIKIPEDIA_AGE_OUTPUT_PATH)

        return data[ARTIST_NAME].unique().tolist()

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        records = await self._collect_records(chunk)
        valid_records = [record for record in records if isinstance(record, dict)]

        if valid_records:
            data = pd.DataFrame.from_records(valid_records)
            append_to_csv(data, WIKIPEDIA_AGE_OUTPUT_PATH)
        else:
            print('No valid records. skipped appending to CSV')

    async def _collect_records(self, artist_names: List[str]) -> List[Dict[str, str]]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artist_names)) as progress_bar:
            func = partial(self._collect_single_artist_age, progress_bar)
            records = await pool.map(func, artist_names)

        return records

    async def _collect_single_artist_age(self, progress_bar: tqdm, artist_name: str) -> Dict[str, str]:
        progress_bar.update(1)
        func = partial(self._wikipedia.page, artist_name)
        page = await run_async(func)
        birth_date, death_date = self._get_birth_and_death_date(page.summary)

        return {
            ARTIST_NAME: artist_name,
            BIRTH_DATE: birth_date,
            DEATH_DATE: death_date
        }

    def _get_birth_and_death_date(self, page_summary: str) -> Tuple[str, str]:
        birth_date = self._extract_normalized_birth_date(page_summary)
        if birth_date:
            return birth_date, ''

        return self._extract_normalized_birth_and_death_date(page_summary)

    def _extract_normalized_birth_date(self, page_summary: str) -> str:
        raw_birth_date = search_between_two_characters(
            start_char=r'(né|born on|born|b\.)',
            end_char=r'\)',
            text=page_summary
        )

        if raw_birth_date:
            raw_results = raw_birth_date[0]
            return self._extract_date(raw_results[-1])
        else:
            return ''

    def _extract_normalized_birth_and_death_date(self, page_summary: str) -> Tuple[str, str]:
        raw_dates = search_between_two_characters(
            start_char=r'\(',
            end_char=r'\)',
            text=page_summary
        )
        if not raw_dates:
            return '', ''

        first_match = raw_dates[0]
        split_dates = first_match.split('–')

        if len(split_dates) == 2:
            dates = [self._extract_date(date) for date in split_dates]
            return dates[0], dates[1]
        else:
            return '', ''

    def _extract_date(self, raw_date: str) -> str:
        split_date = raw_date.split(';')
        clean_date = self._punctuation_regex.sub(' ', split_date[-1])
        stripped_date = clean_date.strip()

        return self._normalize_date(stripped_date)

    @staticmethod
    def _normalize_date(birth_date: str) -> str:
        for datetime_format in WIKIPEDIA_DATETIME_FORMATS:
            try:
                normalized_birth_date = datetime.strptime(birth_date, datetime_format)
                return normalized_birth_date.strftime(DATETIME_FORMAT)

            except ValueError:
                continue

        return ''


if __name__ == '__main__':
    collector = WikipediaAgeCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
