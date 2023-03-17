import asyncio
import os.path
from functools import partial
from typing import List, Dict, Tuple

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE, TRACKS_URL_FORMAT
from consts.data_consts import ARTIST_NAME, ARTISTS, ARTIST_ID
from consts.data_consts import ID
from consts.path_consts import MERGED_DATA_PATH, ARTISTS_IDS_OUTPUT_PATH
from tools.data_chunks_generator import DataChunksGenerator
from utils.file_utils import append_to_csv
from utils.spotify_utils import build_spotify_headers, is_access_token_expired


class ArtistsIDsCollector:
    def __init__(self, chunk_size: int = 50):
        self._session = ClientSession(headers=build_spotify_headers())
        self._chunks_generator = DataChunksGenerator(chunk_size)

    async def collect(self):
        data = pd.read_csv(MERGED_DATA_PATH)
        data.dropna(subset=[ID], inplace=True)
        data.drop_duplicates(subset=[ARTIST_NAME], inplace=True)
        artists_and_track_ids = [(artist, track_id) for artist, track_id in zip(data[ARTIST_NAME], data[ID])]
        existing_artists_and_tracks_ids = self._get_existing_artists_and_tracks()
        chunks = self._chunks_generator.generate_data_chunks(artists_and_track_ids, existing_artists_and_tracks_ids)

        for chunk in chunks:
            await self._collect_single_chunk(chunk)

        await self._session.close()

    async def _collect_single_chunk(self, artists_and_track_ids: List[Tuple[str, str]]) -> None:
        records = await self._get_artists_ids(artists_and_track_ids)
        valid_records = [record for record in records if isinstance(record, dict)]
        artists_ids_data = pd.DataFrame.from_records(valid_records)

        if not artists_ids_data.empty:
            artists_ids_data.dropna(subset=[ARTIST_NAME], inplace=True)

        append_to_csv(data=artists_ids_data, output_path=ARTISTS_IDS_OUTPUT_PATH)

    async def _get_artists_ids(self, artists_and_track_ids: List[Tuple[str, str]]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artists_and_track_ids)) as progress_bar:
            func = partial(self._get_single_track_artist_id, progress_bar)

            return await pool.map(fn=func, iterable=artists_and_track_ids)

    async def _get_single_track_artist_id(self,
                                          progress_bar: tqdm,
                                          artist_and_track_id: Tuple[str, str]) -> Dict[str, str]:
        artist, track_id = artist_and_track_id
        progress_bar.update(1)
        url = TRACKS_URL_FORMAT.format(track_id)

        async with self._session.get(url=url) as raw_response:
            response = await raw_response.json()

        if is_access_token_expired(response):
            await self._renew_client_session()
            return await self._get_single_track_artist_id(progress_bar, artist_and_track_id)

        elif not raw_response.ok:
            return {
                ID: track_id,
                ARTIST_NAME: artist
            }

        else:
            return self._extract_artist_id_from_response(artist, track_id, response)

    @staticmethod
    def _extract_artist_id_from_response(artist: str, track_id: str, response: dict) -> Dict[str, str]:
        record = {
            ID: track_id,
            ARTIST_NAME: artist
        }
        artists = response[ARTISTS]

        if not artists:
            return record

        first_artist = artists[0]
        record[ARTIST_ID] = first_artist[ID]

        return record

    async def _renew_client_session(self) -> None:
        await self._session.close()
        self._session = ClientSession(headers=build_spotify_headers())

    @staticmethod
    def _get_existing_artists_and_tracks() -> List[Tuple[str, str]]:
        if not os.path.exists(ARTISTS_IDS_OUTPUT_PATH):
            return []

        existing_data = pd.read_csv(ARTISTS_IDS_OUTPUT_PATH)
        return [(artist, track_id) for artist, track_id in zip(existing_data[ARTIST_NAME], existing_data[ID])]


if __name__ == '__main__':
    collector = ArtistsIDsCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())