import os.path
from functools import partial
from typing import List, Optional

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import SONG
from consts.genius_consts import GENIUS_API_SEARCH_URL, RESPONSE, RESULT
from consts.path_consts import GENIUS_TRACKS_IDS_OUTPUT_PATH
from consts.shazam_consts import HITS
from data_collection.genius.base_genius_collector import BaseGeniusCollector
from utils.data_utils import extract_column_existing_values, read_merged_data


class GeniusSearchCollector(BaseGeniusCollector):
    def __init__(self, chunk_size: int, max_chunks_number: int, session: Optional[ClientSession] = None):
        super().__init__(chunk_size, max_chunks_number, session)

    async def collect(self) -> None:
        data = self._load_data()
        await self._chunks_generator.execute_by_chunk(
            lst=data[SONG].unique().tolist(),
            filtering_list=extract_column_existing_values(path=GENIUS_TRACKS_IDS_OUTPUT_PATH, column_name=SONG),
            func=self._collect_single_chunk
        )

    @staticmethod
    def _load_data() -> DataFrame:
        data = read_merged_data()
        data.dropna(subset=[SONG], inplace=True)
        data.drop_duplicates(subset=[SONG], inplace=True)

        return data.reset_index(drop=True)

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._fetch_single_song, progress_bar)
            records = await pool.map(func, chunk)

        valid_records = [record for record in records if record is not None]
        data = pd.concat(valid_records).reset_index(drop=True)
        self._append_to_csv(data)

    async def _fetch_single_song(self, progress_bar: tqdm, song: str) -> Optional[DataFrame]:
        progress_bar.update(1)
        params = {'q': song}

        async with self._session.get(url=GENIUS_API_SEARCH_URL, params=params) as raw_response:
            if not raw_response.ok:
                return

            response = await raw_response.json()

        return self._serialize_response(song, response)

    def _serialize_response(self, song: str, response: dict) -> Optional[DataFrame]:
        if not self._is_valid_response(response):
            return

        hits = response.get(RESPONSE, {}).get(HITS)

        if hits:
            return self._serialize_first_hit(song, hits[0])
        else:
            return self._build_empty_result(song)

    def _serialize_first_hit(self, song: str, hit: dict) -> Optional[DataFrame]:
        hit_result = hit.get(RESULT)

        if not hit_result:
            return self._build_empty_result(song)

        hit_result[SONG] = song
        first_hit_data = pd.json_normalize(hit_result)

        if 'featured_artists' in first_hit_data.columns:
            return first_hit_data.drop('featured_artists', axis=1)
        else:
            return first_hit_data

    @staticmethod
    def _build_empty_result(song: str) -> DataFrame:
        return pd.DataFrame({SONG: [song]})

    @staticmethod
    def _append_to_csv(new_data: DataFrame) -> None:
        if os.path.exists(GENIUS_TRACKS_IDS_OUTPUT_PATH):
            existing_data = pd.read_csv(GENIUS_TRACKS_IDS_OUTPUT_PATH)
        else:
            existing_data = pd.DataFrame()

        data = pd.concat([existing_data, new_data]).reset_index(drop=True)

        data.to_csv(GENIUS_TRACKS_IDS_OUTPUT_PATH)
