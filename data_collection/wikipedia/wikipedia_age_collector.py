import asyncio
import os.path
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from typing import List, Dict, Tuple, Callable

import pandas as pd
from asyncio_pool import AioPool
from tqdm import tqdm
from wikipediaapi import Wikipedia

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ARTIST_NAME
from consts.path_consts import WIKIPEDIA_GENDERS_PATH, WIKIPEDIA_AGE_OUTPUT_PATH
from tools.data_chunks_generator import DataChunksGenerator
from utils.datetime_utils import DATETIME_FORMAT
from utils.file_utils import append_to_csv

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
        data = pd.read_csv(WIKIPEDIA_GENDERS_PATH)
        data.dropna(inplace=True)
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=data[ARTIST_NAME].unique().tolist(),
            filtering_list=self._get_existing_artists()
        )

        for chunk in chunks:
            records = await self._collect_records(chunk)
            data = pd.DataFrame.from_records(records)
            append_to_csv(data, WIKIPEDIA_AGE_OUTPUT_PATH)

    async def _collect_records(self, artist_names: List[str]) -> List[Dict[str, str]]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artist_names)) as progress_bar:
            func = partial(self._collect_single_artist_age, progress_bar)
            records = await pool.map(func, artist_names)

        return records

    async def _collect_single_artist_age(self, progress_bar: tqdm, artist_name: str) -> Dict[str, str]:
        progress_bar.update(1)
        func = partial(self._wikipedia.page, artist_name)
        page = await self.run_async(func)

        if page.exists():
            birth_date, death_date = self._get_birth_and_death_date(page.summary)
        else:
            birth_date, death_date = '', ''

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
        raw_birth_date = self._search_between_two_characters(
            start_char=r'born|b\.',
            end_char=r'\)',
            text=page_summary
        )

        if raw_birth_date:
            return self._extract_date(raw_birth_date[0])
        else:
            return ''

    def _extract_normalized_birth_and_death_date(self, page_summary: str) -> Tuple[str, str]:
        raw_dates = self._search_between_two_characters(
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

    @staticmethod
    def _search_between_two_characters(start_char: str, end_char: str, text: str) -> List[str]:
        return re.findall(f"{start_char}(.*?){end_char}", text)

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

    @staticmethod
    async def run_async(func: Callable, max_workers: int = 1):
        with ThreadPoolExecutor(max_workers) as pool:
            return await asyncio.get_event_loop().run_in_executor(pool, func)

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if not os.path.exists(WIKIPEDIA_AGE_OUTPUT_PATH):
            return []

        data = pd.read_csv(WIKIPEDIA_AGE_OUTPUT_PATH)

        return data[ARTIST_NAME].unique().tolist()


if __name__ == '__main__':
    collector = WikipediaAgeCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
