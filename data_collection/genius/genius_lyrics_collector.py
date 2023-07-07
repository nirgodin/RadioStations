import asyncio
import itertools
import os
import re
from functools import partial
from typing import List, Optional, Dict

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from bs4 import BeautifulSoup
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ID
from consts.genius_consts import PATH, GENIUS_LYRICS_URL_FORMAT
from consts.path_consts import GENIUS_TRACKS_IDS_OUTPUT_PATH, GENIUS_LYRICS_OUTPUT_PATH
from data_collection.genius.base_genius_collector import BaseGeniusCollector
from utils.file_utils import read_json, append_dict_to_json
from utils.general_utils import chain_dicts

DATA_LYRICS_CONTAINER = 'data-lyrics-container'


class GeniusLyricsCollector(BaseGeniusCollector):
    def __init__(self, chunk_size: int, max_chunks_number: int):
        super().__init__(chunk_size, max_chunks_number)
        self._lyrics_class_regex = re.compile("^lyrics$|Lyrics__Root")

    async def fetch(self) -> None:
        self._session = await ClientSession().__aenter__()
        chunks = self._chunks_generator.generate_data_chunks(
            lst=list(self._genius_ids_to_lyrics_paths.keys()),
            filtering_list=list(self._existing_songs_lyrics.keys())
        )

        await self._collect_multiple_chunks(chunks)

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._fetch_single_song, progress_bar)
            results = await pool.map(func, chunk)

        valid_results = [result for result in results if result is not None]
        data = chain_dicts(valid_results)

        append_dict_to_json(
            existing_data=self._existing_songs_lyrics,
            new_data=data,
            path=GENIUS_LYRICS_OUTPUT_PATH
        )

    async def _fetch_single_song(self, progress_bar: tqdm, song_id: str) -> Optional[Dict[str, List[str]]]:
        progress_bar.update(1)
        lyrics_path = self._map_song_id_to_lyrics_path(song_id)
        url = GENIUS_LYRICS_URL_FORMAT.format(lyrics_path)

        async with self._session.get(url=url) as raw_response:
            if not raw_response.ok:
                return

            response = await raw_response.text()

        return self._serialize_response(song_id, response)

    def _map_song_id_to_lyrics_path(self, song_id: str) -> str:
        raw_lyrics_path = self._genius_ids_to_lyrics_paths[song_id]
        return raw_lyrics_path[1:] if raw_lyrics_path.startswith('/') else raw_lyrics_path

    def _serialize_response(self, song_id: str, response: str) -> Optional[Dict[str, List[str]]]:
        soup = BeautifulSoup(response, "html.parser")
        div = soup.find("div", class_=self._lyrics_class_regex)
        contents = [content for content in div.contents if content.attrs.get(DATA_LYRICS_CONTAINER) == 'true']
        lyrics_components = [c.contents for c in contents]
        flatten_lyrics_component = itertools.chain.from_iterable(lyrics_components)
        lyrics = [component.text for component in flatten_lyrics_component if component.text]

        return {song_id: lyrics}

    @property
    def _genius_ids_to_lyrics_paths(self) -> Dict[str, str]:
        data = pd.read_csv(GENIUS_TRACKS_IDS_OUTPUT_PATH)
        data.dropna(subset=[ID, PATH], inplace=True)
        data[ID] = data[ID].apply(lambda x: str(int(x)))

        return {track_id: lyrics_path for track_id, lyrics_path in zip(data[ID], data[PATH])}

    @property
    def _existing_songs_lyrics(self) -> Dict[str, List[str]]:
        if not os.path.exists(GENIUS_LYRICS_OUTPUT_PATH):
            return {}

        return read_json(path=GENIUS_LYRICS_OUTPUT_PATH)


async def run_genius_search_fetcher(chunk_size: int = 50, max_chunks_number: int = 10) -> None:
    async with GeniusLyricsCollector(chunk_size, max_chunks_number) as fetcher:
        await fetcher.fetch()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_genius_search_fetcher(max_chunks_number=2))
