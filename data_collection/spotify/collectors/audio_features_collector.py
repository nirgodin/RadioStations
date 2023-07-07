import os.path
from functools import partial
from typing import Union, Tuple, List

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AUDIO_FEATURES_URL_FORMAT, AIO_POOL_SIZE
from consts.data_consts import NAME, ARTIST_NAME, TRACKS, ITEMS, URI, TRACK
from consts.env_consts import SPOTIFY_AUDIO_FEATURES_DRIVE_ID
from consts.path_consts import MERGED_DATA_PATH, AUDIO_FEATURES_CHUNK_OUTPUT_PATH_FORMAT, AUDIO_FEATURES_DATA_PATH
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from tools.data_chunks_generator import DataChunksGenerator
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.data_utils import extract_column_existing_values
from utils.datetime_utils import get_current_datetime
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import to_csv
from utils.spotify_utils import get_spotipy, build_spotify_query


class AudioFeaturesCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        super().__init__(session, chunk_size, max_chunks_number)
        self._sp = get_spotipy()
        self._chunks_generator = DataChunksGenerator(chunk_size)

    async def collect(self, **kwargs) -> None:
        data = pd.read_csv(MERGED_DATA_PATH)
        data.drop_duplicates(subset=[NAME, ARTIST_NAME], inplace=True)
        artists_and_tracks = [(artist, track) for artist, track in zip(data[ARTIST_NAME], data[NAME])]
        existing_artists_and_tracks = extract_column_existing_values(AUDIO_FEATURES_DATA_PATH, [ARTIST_NAME, NAME])
        chunks = self._chunks_generator.generate_data_chunks(
            lst=artists_and_tracks,
            filtering_list=existing_artists_and_tracks
        )

        await self._collect_multiple_chunks(chunks)

    @staticmethod
    def _get_existing_tracks_and_artists() -> List[Tuple[str, str]]:
        if not os.path.exists(AUDIO_FEATURES_DATA_PATH):
            return []

        existing_data = pd.read_csv(AUDIO_FEATURES_DATA_PATH)
        existing_data.dropna(subset=[NAME, ARTIST_NAME], inplace=True)

        return [(artist, track) for artist, track in zip(existing_data[ARTIST_NAME], existing_data[NAME])]

    async def _collect_single_chunk(self, chunk: List[Tuple[str, str]]) -> None:
        tracks_features = await self._get_tracks_features(chunk)
        valid_features = [feature for feature in tracks_features if isinstance(feature, dict)]
        print(f'Failed to collect audio features for {len(tracks_features) - len(valid_features)} out of {len(tracks_features)} tracks')
        tracks_features_data = pd.DataFrame.from_records(valid_features)

        self._output_results(tracks_features_data)

    async def _get_tracks_features(self, chunk: List[Tuple[str, str]]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._get_single_track_features, progress_bar)

            return await pool.map(fn=func, iterable=chunk)

    async def _get_single_track_features(self,
                                         progress_bar: tqdm,
                                         artist_and_track: Tuple[str, str]) -> dict:
        progress_bar.update(1)
        artist, track = artist_and_track
        url = self._build_request_url(artist, track)

        async with self._session.get(url=url) as response:
            audio_features_response = await response.json()

        if self._is_access_token_expired(audio_features_response):
            await self._renew_client_session()
            return await self._get_single_track_features(progress_bar, artist_and_track)

        audio_features_response[ARTIST_NAME] = artist
        audio_features_response[NAME] = track

        return audio_features_response

    def _build_request_url(self, artist: str, track: str) -> str:
        track_id = self._get_track_id(artist, track)
        return AUDIO_FEATURES_URL_FORMAT.format(track_id)

    def _get_track_id(self, artist: str, track: str) -> Union[dict, None]:
        query = build_spotify_query(artist, track)
        query_result = self._sp.search(q=query, type=TRACK)
        track_uri = query_result[TRACKS][ITEMS][0][URI]
        split_uri = track_uri.split(':')

        return split_uri[-1]

    @staticmethod
    def _output_results(tracks_features_data: DataFrame) -> None:
        now = get_current_datetime()
        output_path = AUDIO_FEATURES_CHUNK_OUTPUT_PATH_FORMAT.format(now)
        to_csv(data=tracks_features_data, output_path=output_path)
        file_metadata = GoogleDriveUploadMetadata(
            local_path=output_path,
            drive_folder_id=os.environ[SPOTIFY_AUDIO_FEATURES_DRIVE_ID]
        )
        upload_files_to_drive(file_metadata)
