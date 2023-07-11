import asyncio
import os.path
from functools import partial
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from asyncio_pool import AioPool
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.api_consts import AIO_POOL_SIZE
from consts.language_consts import LANGUAGE, SCORE
from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, SHAZAM_TRACKS_LANGUAGES_PATH, SHAZAM_LYRICS_EMBEDDINGS_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY
from data_collection.openai.openai_client import OpenAIClient
from tools.data_chunks_generator import DataChunksGenerator
from tools.environment_manager import EnvironmentManager
from tools.language_detector import LanguageDetector
from utils.data_utils import extract_column_existing_values
from utils.file_utils import read_json, append_to_csv


class ShazamLyricsEmbeddingsFetcher:
    def __init__(self,
                 chunk_size: int = 50,
                 chunks_limit: Optional[int] = None,
                 openai_client: Optional[OpenAIClient] = None):
        self._chunk_size = chunk_size
        self._chunks_limit = chunks_limit
        self._openai_client = openai_client
        self._data_chunks_generator = DataChunksGenerator(self._chunk_size)
        self._shazam_tracks_lyrics: Dict[str, List[str]] = read_json(SHAZAM_TRACKS_LYRICS_PATH)

    async def fetch(self) -> None:
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=list(self._shazam_tracks_lyrics.keys()),
            filtering_list=self._get_existing_tracks_ids()
        )

        for i, chunk in enumerate(chunks):
            if i == self._chunks_limit:
                break
            else:
                await self._extract_single_chunk_tracks_languages(chunk)

    @staticmethod
    def _get_existing_tracks_ids() -> List[str]:
        tracks_keys = extract_column_existing_values(SHAZAM_LYRICS_EMBEDDINGS_PATH, SHAZAM_TRACK_KEY)
        return [str(int(track_key)) for track_key in tracks_keys]

    async def _extract_single_chunk_tracks_languages(self, chunk: List[str]) -> None:
        records = await self._get_tracks_embeddings_records(chunk)
        data = pd.DataFrame.from_records(records)

        append_to_csv(data=data, output_path=SHAZAM_LYRICS_EMBEDDINGS_PATH)

    async def _get_tracks_embeddings_records(self, chunk: List[str]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._extract_single_track_embeddings, progress_bar)
            records = await pool.map(func, chunk)

        return records

    async def _extract_single_track_embeddings(self, progress_bar: tqdm, track_id: str) -> dict:
        track_lyrics = self._shazam_tracks_lyrics[track_id]
        record = {SHAZAM_TRACK_KEY: track_id}

        if not track_lyrics:
            return record

        track_concatenated_lyrics = '\n'.join(track_lyrics)
        embeddings = await self._openai_client.embeddings(track_concatenated_lyrics)
        embeddings_record = {f'lyrics_embedding_{i+1}': embedding for i, embedding in enumerate(embeddings)}
        record.update(embeddings_record)
        progress_bar.update(1)

        return record

    async def __aenter__(self) -> 'ShazamLyricsEmbeddingsFetcher':
        self._openai_client = await OpenAIClient().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._openai_client:
            await self._openai_client.__aexit__(exc_type, exc_val, exc_tb)


async def run():
    EnvironmentManager().set_env_variables()
    async with ShazamLyricsEmbeddingsFetcher() as fetcher:
        await fetcher.fetch()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(run())
