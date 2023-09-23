import asyncio
import os.path
from functools import partial
from typing import List, Dict, Tuple

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE, SEARCH_URL
from consts.data_consts import ARTIST_NAME, NAME, TRACK, TYPE, TRACKS, ITEMS, URI, SONG
from consts.data_consts import ID
from consts.env_consts import SPOTIFY_TRACKS_IDS_DRIVE_ID
from consts.path_consts import TRACKS_IDS_OUTPUT_PATH
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from tools.environment_manager import EnvironmentManager
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.data_utils import extract_column_existing_values, read_merged_data
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import append_to_csv
from utils.spotify_utils import build_spotify_headers, build_spotify_query


class TracksIDsCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        super().__init__(session, chunk_size, max_chunks_number)

    async def collect(self, **kwargs):
        EnvironmentManager().set_env_variables()
        data = read_merged_data()
        missing_ids_data = data[data[ID].isna()]
        missing_ids_data.drop_duplicates(subset=[SONG], inplace=True)
        artists_and_tracks_names = self._extract_artists_and_tracks_names(missing_ids_data)
        existing_artists_and_tracks_names = extract_column_existing_values(TRACKS_IDS_OUTPUT_PATH, [ARTIST_NAME, NAME])

        await self._collect_multiple_chunks(artists_and_tracks_names, existing_artists_and_tracks_names)

    async def _collect_single_chunk(self, chunk: List[Tuple[str, str]]) -> None:
        records = await self._get_tracks_ids(chunk)
        valid_records = [record for record in records if isinstance(record, dict)]
        tracks_ids_data = pd.DataFrame.from_records(valid_records)

        if not tracks_ids_data.empty:
            tracks_ids_data.dropna(subset=[NAME, ARTIST_NAME], inplace=True)

        self._output_results(tracks_ids_data)

    async def _get_tracks_ids(self, artists_and_tracks_names: List[Tuple[str, str]]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artists_and_tracks_names)) as progress_bar:
            func = partial(self._get_single_track_id, progress_bar)

            return await pool.map(fn=func, iterable=artists_and_tracks_names)

    async def _get_single_track_id(self,
                                   progress_bar: tqdm,
                                   artist_and_track_name: Tuple[str, str]) -> Dict[str, str]:
        progress_bar.update(1)
        artist, track_name = artist_and_track_name
        params = {
            'q': build_spotify_query(artist, track_name),
            TYPE: [TRACK]
        }

        async with self._session.get(url=SEARCH_URL, params=params) as raw_response:
            if not raw_response.ok:
                return {
                    NAME: track_name,
                    ARTIST_NAME: artist
                }
            response = await raw_response.json()

        if self._is_access_token_expired(response):
            await self._renew_client_session()
            return await self._get_single_track_id(progress_bar, artist_and_track_name)

        else:
            return self._extract_track_id_and_uri_from_response(artist, track_name, response)

    @staticmethod
    def _extract_track_id_and_uri_from_response(artist: str, track_name: str, response: dict) -> Dict[str, str]:
        record = {
            NAME: track_name,
            ARTIST_NAME: artist
        }
        items = response.get(TRACKS, {}).get(ITEMS, [])

        if not items:
            return record

        first_track = items[0]
        record[ID] = first_track[ID]
        record[URI] = first_track[URI]

        return record

    @staticmethod
    def _extract_artists_and_tracks_names(data: DataFrame) -> List[Tuple[str, str]]:
        return [(artist, track_name) for artist, track_name in zip(data[ARTIST_NAME], data[NAME])]

    @staticmethod
    def _output_results(tracks_ids_data: DataFrame) -> None:
        append_to_csv(data=tracks_ids_data, output_path=TRACKS_IDS_OUTPUT_PATH)
        file_metadata = GoogleDriveUploadMetadata(
            local_path=TRACKS_IDS_OUTPUT_PATH,
            drive_folder_id=os.environ[SPOTIFY_TRACKS_IDS_DRIVE_ID]
        )
        upload_files_to_drive(file_metadata)


if __name__ == '__main__':
    session = ClientSession(headers=build_spotify_headers())
    collector = TracksIDsCollector(session, 50, 5)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
    asyncio.run(session.close())
