import asyncio
import os.path
from functools import partial
from typing import List, Optional

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE, ARTISTS_ALBUMS_URL_FORMAT
from consts.data_consts import ARTIST_ID, ITEMS, NEXT
from consts.env_consts import SPOTIFY_ALBUMS_DETAILS_DRIVE_ID
from consts.path_consts import ARTISTS_IDS_OUTPUT_PATH, ALBUMS_DETAILS_OUTPUT_PATH
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from tools.data_chunks_generator import DataChunksGenerator
from tools.google_drive.google_drive_file_metadata import GoogleDriveFileMetadata
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import append_to_csv
from utils.spotify_utils import build_spotify_headers


class AlbumsDetailsCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int):
        super().__init__(session, chunk_size)
        self._chunks_generator = DataChunksGenerator(chunk_size)

    async def collect(self):
        data = pd.read_csv(ARTISTS_IDS_OUTPUT_PATH)
        data.dropna(subset=[ARTIST_ID], inplace=True)
        artists_ids = data[ARTIST_ID].unique().tolist()
        existing_artists_ids = self._get_existing_artists_ids()
        chunks = self._chunks_generator.generate_data_chunks(artists_ids, existing_artists_ids)

        for chunk in chunks:
            await self._collect_single_chunk(chunk)

        await self._session.close()

    async def _collect_single_chunk(self, artists_ids: List[str]) -> None:
        artists_dfs = await self._get_artists_albums(artists_ids)
        valid_dfs = [df for df in artists_dfs if isinstance(df, DataFrame)]

        if valid_dfs:
            albums_data = pd.concat(valid_dfs)
            append_to_csv(data=albums_data, output_path=ALBUMS_DETAILS_OUTPUT_PATH)
        else:
            print('No valid dfs. Skipping append to csv')

    async def _get_artists_albums(self, artists_ids: List[str]) -> List[DataFrame]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(artists_ids)) as progress_bar:
            func = partial(self._get_single_artist_albums, progress_bar)

            return await pool.map(fn=func, iterable=artists_ids)

    async def _get_single_artist_albums(self,
                                        progress_bar: tqdm,
                                        artist_id: str) -> DataFrame:
        progress_bar.update(1)
        url = ARTISTS_ALBUMS_URL_FORMAT.format(artist_id)
        albums_pages = await self._fetch_albums_pages(url=url, albums_pages=[])
        albums_data = self._extract_albums_details(albums_pages)

        if not albums_data.empty:
            albums_data[ARTIST_ID] = artist_id

        return albums_data

    async def _fetch_albums_pages(self, url: str, albums_pages: List[dict]) -> List[dict]:
        while url is not None:
            async with self._session.get(url=url) as raw_response:
                if raw_response.ok:
                    response = await raw_response.json()
                    albums_pages.append(response)
                    url = response[NEXT]

                elif self._is_access_token_expired(response):
                    await self._renew_client_session()
                    return await self._fetch_albums_pages(url, albums_pages)

                else:
                    url = None

        return albums_pages

    def _extract_albums_details(self, albums_pages: List[dict]) -> DataFrame:
        artist_albums = []

        for page in albums_pages:
            page_items = self._extract_single_album_page_items(page)

            if page_items is not None:
                artist_albums.extend(page_items)

        return pd.DataFrame.from_records(artist_albums)

    @staticmethod
    def _extract_single_album_page_items(albums_page: dict) -> Optional[List[dict]]:
        if not isinstance(albums_page, dict):
            return

        return albums_page.get(ITEMS)

    @staticmethod
    def _get_existing_artists_ids() -> List[str]:
        if not os.path.exists(ALBUMS_DETAILS_OUTPUT_PATH):
            return []

        existing_data = pd.read_csv(ALBUMS_DETAILS_OUTPUT_PATH)
        return existing_data[ARTIST_ID].tolist()

    @staticmethod
    def _output_results(albums_data: DataFrame) -> None:
        append_to_csv(data=albums_data, output_path=ALBUMS_DETAILS_OUTPUT_PATH)
        file_metadata = GoogleDriveFileMetadata(
            local_path=ALBUMS_DETAILS_OUTPUT_PATH,
            drive_folder_id=os.environ[SPOTIFY_ALBUMS_DETAILS_DRIVE_ID]
        )
        upload_files_to_drive(file_metadata)


if __name__ == '__main__':
    collector = AlbumsDetailsCollector(session=ClientSession(headers=build_spotify_headers()), chunk_size=50)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
