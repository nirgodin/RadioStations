import copy
import re
from abc import abstractmethod, ABC
from datetime import datetime
from functools import partial
from typing import List, Dict, Tuple

import pandas as pd
from asyncio_pool import AioPool
from tqdm import tqdm

from component_factory import ComponentFactory
from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ARTIST_NAME
from consts.datetime_consts import DATETIME_FORMAT
from consts.path_consts import WIKIPEDIA_AGE_OUTPUT_PATH, HEBREW_MONTHS_MAPPING_PATH
from consts.wikipedia_consts import WIKIPEDIA_DATETIME_FORMATS, BIRTH_DATE, DEATH_DATE
from tools.data_chunks_generator import DataChunksGenerator
from utils.callable_utils import run_async
from utils.file_utils import append_to_csv, read_json
from utils.regex_utils import search_between_two_characters, contains_any_hebrew_character


class BaseWikipediaAgeCollector(ABC):
    def __init__(self):
        self._punctuation_regex = re.compile(r'[^A-Za-z0-9]+')
        self._data_chunks_generator = DataChunksGenerator()
        self._hebrew_months_mapping = read_json(HEBREW_MONTHS_MAPPING_PATH)

    async def collect(self):
        await self._data_chunks_generator.execute_by_chunk(
            lst=self._get_contender_artists(),
            filtering_list=self._get_existing_artists(),
            func=self._collect_single_chunk
        )

    @abstractmethod
    def _get_contender_artists(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def _get_existing_artists(self) -> List[str]:
        raise NotImplementedError

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        records = await self._collect_records(chunk)
        valid_records = [record for record in records if isinstance(record, dict)]

        if valid_records:
            data = pd.DataFrame.from_records(valid_records)
            append_to_csv(data, WIKIPEDIA_AGE_OUTPUT_PATH)
        else:
            print('No valid records. skipped appending to CSV')

    async def _collect_records(self, artists_details: List[str]) -> List[Dict[str, str]]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artists_details)) as progress_bar:
            func = partial(self._collect_single_artist_age, progress_bar)
            records = await pool.map(func, artists_details)

        return records

    async def _collect_single_artist_age(self, progress_bar: tqdm, artist_detail: str) -> Dict[str, str]:
        progress_bar.update(1)
        artist_page_name = self._get_artist_wikipedia_name(artist_detail)
        wikipedia_abbreviation = self._get_wikipedia_abbreviation(artist_detail)
        wikipedia = ComponentFactory.get_wikipedia(wikipedia_abbreviation)
        func = partial(wikipedia.page, artist_page_name)
        page = await run_async(func)
        birth_date, death_date = self._get_birth_and_death_date(page.summary)

        return {
            ARTIST_NAME: self._get_artist_spotify_name(artist_detail),
            BIRTH_DATE: birth_date,
            DEATH_DATE: death_date
        }

    @abstractmethod
    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get_artist_spotify_name(self, artist_detail: str) -> str:
        raise NotImplementedError

    def _get_birth_and_death_date(self, page_summary: str) -> Tuple[str, str]:
        birth_date = self._extract_normalized_birth_date(page_summary)
        if birth_date:
            return birth_date, ''

        return self._extract_normalized_birth_and_death_date(page_summary)

    def _extract_normalized_birth_date(self, page_summary: str) -> str:
        raw_birth_date = search_between_two_characters(
            start_char=r'(ב\-|né|born on|born|b\.)',
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
        if contains_any_hebrew_character(raw_date):
            stripped_date = self._extract_hebrew_date(raw_date)

        else:
            split_date = raw_date.split(';')
            clean_date = self._punctuation_regex.sub(' ', split_date[-1])
            stripped_date = clean_date.strip()

        return self._normalize_date(stripped_date)

    def _extract_hebrew_date(self, raw_date: str) -> str:
        modified_date = copy.deepcopy(raw_date)

        for hebrew_month, english_month in self._hebrew_months_mapping.items():
            modified_date = modified_date.replace(hebrew_month, english_month)

            if modified_date != raw_date:
                break

        return self._punctuation_regex.sub(' ', modified_date).strip()

    @staticmethod
    def _normalize_date(birth_date: str) -> str:
        for datetime_format in WIKIPEDIA_DATETIME_FORMATS:
            try:
                normalized_birth_date = datetime.strptime(birth_date, datetime_format)
                return normalized_birth_date.strftime(DATETIME_FORMAT)

            except ValueError:
                continue

        return ''
