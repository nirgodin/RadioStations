import asyncio
from functools import partial
from typing import List, Optional

import pandas as pd
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import SONG
from consts.genius_consts import GENIUS_API_SEARCH_URL, RESPONSE, RESULT
from consts.path_consts import MERGED_DATA_PATH, GENIUS_TRACKS_IDS_OUTPUT_PATH
from consts.shazam_consts import HITS
from data_collection.genius.base_genius_collector import BaseGeniusCollector
from utils.data_utils import extract_column_existing_values
from utils.file_utils import append_to_csv


class GeniusSearchCollector(BaseGeniusCollector):
    def __init__(self, chunk_size: int, max_chunks_number: int):
        super().__init__(chunk_size, max_chunks_number)

    async def fetch(self) -> None:
        data = self._load_data()
        chunks = self._chunks_generator.generate_data_chunks(
            lst=data[SONG].unique().tolist(),
            filtering_list=extract_column_existing_values(path=GENIUS_TRACKS_IDS_OUTPUT_PATH, column_name=SONG)
        )

        await self._collect_multiple_chunks(chunks)

    @staticmethod
    def _load_data() -> DataFrame:
        data = pd.read_csv(MERGED_DATA_PATH)
        data.dropna(subset=[SONG], inplace=True)
        data.drop_duplicates(subset=[SONG], inplace=True)
        data.reset_index(drop=True, inplace=True)

        return data

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._fetch_single_song, progress_bar)
            records = await pool.map(func, chunk)

        valid_records = [record for record in records if record is not None]
        data = pd.concat(valid_records).reset_index(drop=True)
        append_to_csv(data, output_path=GENIUS_TRACKS_IDS_OUTPUT_PATH)

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
        return pd.json_normalize(hit_result)

    @staticmethod
    def _build_empty_result(song: str) -> DataFrame:
        return pd.DataFrame({SONG: [song]})


async def run_genius_search_fetcher(chunk_size: int = 50, max_chunks_number: int = 10) -> None:
    async with GeniusSearchCollector(chunk_size, max_chunks_number) as fetcher:
        await fetcher.fetch()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_genius_search_fetcher(max_chunks_number=2))
