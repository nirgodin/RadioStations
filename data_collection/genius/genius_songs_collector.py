import asyncio
import os
from functools import partial
from typing import List, Optional, Dict

import pandas as pd
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import SONG, ID
from consts.genius_consts import META, STATUS, RESPONSE, RESULT, GENIUS_API_SONG_URL_FORMAT
from consts.path_consts import GENIUS_TRACKS_IDS_OUTPUT_PATH, GENIUS_SONGS_OUTPUT_PATH
from consts.shazam_consts import HITS
from data_collection.genius.base_genius_collector import BaseGeniusCollector
from utils.file_utils import append_to_csv, read_json, append_dict_to_json
from utils.general_utils import chain_dicts

IRRELEVANT_KEYS = [
    'annotation_count',
    'api_path',
    'artist_names',
    'full_title',
    'header_image_thumbnail_url',
    'header_image_url',
    ID,
    'lyrics_owner_id',
    'lyrics_state',
    'path',
    'pyongs_count',
    'relationships_index_url',
    'release_date_for_display',
    'release_date_with_abbreviated_month_for_display',
    'song_art_image_thumbnail_url',
    'song_art_image_url',
    'title',
    'title_with_featured',
    'url',
    'featured_artists',
    'primary_artist'
]


class GeniusSongsCollector(BaseGeniusCollector):
    def __init__(self, chunk_size: int, max_chunks_number: int):
        super().__init__(chunk_size, max_chunks_number)

    async def fetch(self) -> None:
        data = self._load_data()
        chunks = self._chunks_generator.generate_data_chunks(
            lst=data[ID].unique().tolist(),
            filtering_list=list(self._existing_songs.keys())
        )

        await self._collect_multiple_chunks(chunks)

    @staticmethod
    def _load_data() -> DataFrame:
        data = pd.read_csv(GENIUS_TRACKS_IDS_OUTPUT_PATH)
        data.dropna(subset=ID, inplace=True)
        data[ID] = data[ID].apply(lambda x: str(int(x)))

        return data

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._collect_single_song, progress_bar)
            results = await pool.map(func, chunk)

        valid_results = [result for result in results if result is not None]
        data = chain_dicts(valid_results)

        append_dict_to_json(
            existing_data=self._existing_songs,
            new_data=data,
            path=GENIUS_SONGS_OUTPUT_PATH
        )

    async def _collect_single_song(self, progress_bar: tqdm, song_id: str) -> Optional[DataFrame]:
        progress_bar.update(1)
        url = GENIUS_API_SONG_URL_FORMAT.format(song_id)

        async with self._session.get(url=url) as raw_response:
            if not raw_response.ok:
                return

            response = await raw_response.json()

        return self._serialize_response(song_id, response)

    def _serialize_response(self, song_id: str, response: dict) -> Optional[Dict[str, dict]]:
        if not self._is_valid_response(response):
            return

        song = response.get(RESPONSE, {}).get(SONG)

        if song:
            formatted_song = {k: v for k, v in song.items() if k not in IRRELEVANT_KEYS}
            return {song_id: formatted_song}
        else:
            return {song_id: {}}

    @property
    def _existing_songs(self) -> Dict[str, dict]:
        if not os.path.exists(GENIUS_SONGS_OUTPUT_PATH):
            return {}

        return read_json(path=GENIUS_SONGS_OUTPUT_PATH)


async def run_genius_search_fetcher(chunk_size: int = 50, max_chunks_number: int = 10) -> None:
    async with GeniusSongsCollector(chunk_size, max_chunks_number) as fetcher:
        await fetcher.fetch()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_genius_search_fetcher(max_chunks_number=2))
