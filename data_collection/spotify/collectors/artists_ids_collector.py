import asyncio
import os.path
from functools import partial
from typing import List, Dict, Tuple

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE, TRACKS_URL_FORMAT
from consts.data_consts import ARTIST_NAME, ARTISTS, ARTIST_ID
from consts.data_consts import ID
from consts.env_consts import SPOTIFY_ARTISTS_IDS_DRIVE_ID
from consts.path_consts import MERGED_DATA_PATH, ARTISTS_IDS_OUTPUT_PATH
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from tools.data_chunks_generator import DataChunksGenerator
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.data_utils import extract_column_existing_values
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import append_to_csv
from utils.spotify_utils import build_spotify_headers


class ArtistsIDsCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        super().__init__(session, chunk_size, max_chunks_number)
        self._chunks_generator = DataChunksGenerator(chunk_size)

    async def collect(self, **kwargs):
        data = pd.read_csv(MERGED_DATA_PATH)
        data.dropna(subset=[ID], inplace=True)
        data.drop_duplicates(subset=[ARTIST_NAME], inplace=True)
        artists_and_track_ids = [(artist, track_id) for artist, track_id in zip(data[ARTIST_NAME], data[ID])]
        existing_artists_and_tracks_ids = extract_column_existing_values(ARTISTS_IDS_OUTPUT_PATH, [ARTIST_NAME, ID])
        chunks = self._chunks_generator.generate_data_chunks(artists_and_track_ids, existing_artists_and_tracks_ids)

        await self._collect_multiple_chunks(chunks)

    async def _collect_single_chunk(self, chunk: List[Tuple[str, str]]) -> None:
        records = await self._get_artists_ids(chunk)
        valid_records = [record for record in records if isinstance(record, dict)]
        artists_ids_data = pd.DataFrame.from_records(valid_records)

        if not artists_ids_data.empty:
            artists_ids_data.dropna(subset=[ARTIST_NAME], inplace=True)

        self._output_results(artists_ids_data)

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

        if self._is_access_token_expired(response):
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

    @staticmethod
    def _output_results(artists_ids_data: DataFrame) -> None:
        append_to_csv(data=artists_ids_data, output_path=ARTISTS_IDS_OUTPUT_PATH)
        file_metadata = GoogleDriveUploadMetadata(
            local_path=ARTISTS_IDS_OUTPUT_PATH,
            drive_folder_id=os.environ[SPOTIFY_ARTISTS_IDS_DRIVE_ID]
        )
        upload_files_to_drive(file_metadata)


if __name__ == '__main__':
    session = ClientSession(headers=build_spotify_headers())
    collector = ArtistsIDsCollector(session, 100, 2)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
    session.close()
