import asyncio
import os.path
from functools import partial
from typing import List, Dict, Optional

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE, TRACKS_URL_FORMAT
from consts.data_consts import ALBUM_ID, ALBUM, ALBUM_GROUP, \
    ALBUM_TYPE, ARTIST_ID, ARTISTS
from consts.data_consts import ID
from consts.env_consts import SPOTIFY_TRACKS_IDS_DRIVE_ID
from consts.path_consts import TRACKS_IDS_OUTPUT_PATH, TRACKS_ALBUMS_DETAILS_OUTPUT_PATH
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from tools.environment_manager import EnvironmentManager
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.data_utils import extract_column_existing_values, read_merged_data
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import append_to_csv
from utils.spotify_utils import build_spotify_headers


class TracksAlbumsDetailsCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        super().__init__(session, chunk_size, max_chunks_number)

    async def collect(self, **kwargs):
        EnvironmentManager().set_env_variables()
        data = read_merged_data()
        missing_album_ids_data = data[data[ALBUM_ID].isna()]
        missing_album_ids_data.drop_duplicates(subset=[ID], inplace=True)
        tracks_ids = data[ID].unique().tolist()
        existing_tracks_ids = extract_column_existing_values(TRACKS_ALBUMS_DETAILS_OUTPUT_PATH, ID)

        await self._collect_multiple_chunks(tracks_ids, existing_tracks_ids)

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        records = await self._get_albums_details(chunk)
        valid_records = [record for record in records if isinstance(record, dict)]
        albums_details_data = pd.DataFrame.from_records(valid_records)

        if not albums_details_data.empty:
            albums_details_data.dropna(subset=[ID], inplace=True)

        if not albums_details_data.empty:
            print(f"Found {len(albums_details_data)} non empty results. Appending to existing data")
            self._output_results(albums_details_data)

    async def _get_albums_details(self, tracks_ids: List[str]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(tracks_ids)) as progress_bar:
            func = partial(self._get_single_track_album_details, progress_bar)

            return await pool.map(fn=func, iterable=tracks_ids)

    async def _get_single_track_album_details(self, progress_bar: tqdm, track_id: str) -> Dict[str, str]:
        progress_bar.update(1)
        url = TRACKS_URL_FORMAT.format(track_id)

        async with self._session.get(url=url) as raw_response:
            if not raw_response.ok:
                return {}

            response = await raw_response.json()

        if self._is_access_token_expired(response):
            await self._renew_client_session()
            return await self._get_single_track_album_details(progress_bar, track_id)

        return self._extract_album_details(track_id, response)

    def _extract_album_details(self, track_id: str, response: dict) -> Dict[str, str]:
        album = response.get(ALBUM)
        artist_id = self._extract_artist_id(response)

        if album is None or artist_id is None:
            return {}

        album[ALBUM_ID] = album.pop(ID)
        album[ALBUM_GROUP] = album[ALBUM_TYPE]
        album[ARTIST_ID] = response.get(ARTISTS)
        album[ID] = track_id

        return album

    @staticmethod
    def _extract_artist_id(response: dict) -> Optional[str]:
        artists = response.get(ARTISTS)

        if artists:
            main_artist = artists[0]
            return main_artist.get(ID)

    @staticmethod
    def _output_results(albums_details_data: DataFrame) -> None:
        append_to_csv(data=albums_details_data, output_path=TRACKS_ALBUMS_DETAILS_OUTPUT_PATH)
        file_metadata = GoogleDriveUploadMetadata(
            local_path=TRACKS_IDS_OUTPUT_PATH,
            drive_folder_id=os.environ[SPOTIFY_TRACKS_IDS_DRIVE_ID]
        )
        upload_files_to_drive(file_metadata)


if __name__ == '__main__':
    session = ClientSession(headers=build_spotify_headers())
    collector = TracksAlbumsDetailsCollector(session, 50, 2)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
    asyncio.run(session.close())
