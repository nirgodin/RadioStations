import asyncio
import concurrent
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from typing import List, Optional, Dict, Tuple, Callable

import pandas as pd
from asyncio_pool import AioPool
from tqdm import tqdm
from wikipediaapi import Wikipedia, WikipediaPage

WIKIPEDIA_DATETIME_FORMATS = [
    '%B %d %Y',
    '%d %B %Y'
]
DATETIME_FORMAT = '%Y_%m_%d %H_%M_%S_%f'
BIRTH_DATE = 'birth_date'
DEATH_DATE = 'death_date'
ARTIST_NAME = 'artist_name'
AIO_POOL_SIZE = 5


class WikipediaAgeCollector:
    def __init__(self):
        self._wikipedia = Wikipedia('en')
        self._punctuation_regex = re.compile(r'[^A-Za-z0-9]+')

    async def collect(self, artist_names: List[str]):
        records = await self._collect_records(artist_names)
        data = pd.DataFrame.from_records(records)
        print('b')

    async def _collect_records(self, artist_names: List[str]) -> List[Dict[str, str]]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artist_names)) as progress_bar:
            func = partial(self._collect_single_artist_age, progress_bar)
            records = await pool.map(func, artist_names)

        return records

    async def _collect_single_artist_age(self, progress_bar: tqdm, artist_name: str) -> Dict[str, str]:
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
            start_char='born',
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
        clean_date = self._punctuation_regex.sub(' ', raw_date)
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


if __name__ == '__main__':
    collector = WikipediaAgeCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect(['Amy Winehouse', 'David Guetta', 'Neil Young', 'Billie Eilish', 'Aviv Geffen']))
