import itertools
import os
import re
from functools import partial
from typing import List, Optional, Dict, Generator

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from bs4 import BeautifulSoup
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ID
from consts.genius_consts import PATH, GENIUS_LYRICS_URL_FORMAT, DATA_LYRICS_CONTAINER
from consts.path_consts import GENIUS_TRACKS_IDS_OUTPUT_PATH, GENIUS_LYRICS_OUTPUT_PATH
from data_collection.genius.base_genius_collector import BaseGeniusCollector
from tools.data_chunks_generator import DataChunksGenerator
from utils.file_utils import read_json, append_dict_to_json
from utils.general_utils import chain_dicts


class GeniusLyricsCollector(BaseGeniusCollector):
    def __init__(self, chunk_size: int, max_chunks_number: int, session: Optional[ClientSession] = None):
        super().__init__(chunk_size, max_chunks_number, session)
        self._session = session
        self._lyrics_class_regex = re.compile("^lyrics$|Lyrics__Root")
        self._chunks_generator = DataChunksGenerator()

    async def collect(self) -> None:
        chunks = self._chunks_generator.generate_data_chunks(
            lst=list(self._genius_ids_to_lyrics_paths.keys()),
            filtering_list=list(self._existing_songs_lyrics.keys())
        )

        await self._collect_multiple_chunks(chunks)

    async def _collect_multiple_chunks(self, chunks: Generator[list, None, None]) -> None:
        for chunk_number, chunk in enumerate(chunks):
            if chunk_number + 1 < self._max_chunks_number:
                await self._collect_single_chunk(chunk)
            else:
                break

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._fetch_single_song, progress_bar)
            results = await pool.map(func, chunk)

        valid_results = [result for result in results if isinstance(result, dict)]
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

    def _serialize_response(self, song_id: str, response: str) -> Dict[str, List[str]]:
        soup = BeautifulSoup(response, "html.parser")
        div = soup.find("div", class_=self._lyrics_class_regex)
        contents = [content for content in div.contents if self._is_lyrics_container(content)]
        lyrics_components = [component.contents for component in contents]
        flatten_lyrics_component = itertools.chain.from_iterable(lyrics_components)
        lyrics = [component.text for component in flatten_lyrics_component if component.text]

        return {song_id: lyrics}

    @staticmethod
    def _is_lyrics_container(content) -> bool:
        return content.attrs.get(DATA_LYRICS_CONTAINER) == 'true'

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

    async def __aenter__(self) -> 'GeniusLyricsCollector':
        self._session = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.__aexit__(exc_type, exc_val, exc_tb)
