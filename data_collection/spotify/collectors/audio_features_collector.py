import os.path
from functools import partial
from typing import Union, Tuple, List

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import AUDIO_FEATURES_URL_FORMAT, AIO_POOL_SIZE
from consts.data_consts import NAME, ARTIST_NAME, TRACKS, ITEMS, URI
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, AUDIO_FEATURES_CHUNK_OUTPUT_PATH_FORMAT, AUDIO_FEATURES_DATA_PATH
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from tools.data_chunks_generator import DataChunksGenerator
from utils.datetime_utils import get_current_datetime
from utils.spotify_utils import get_spotipy


class AudioFeaturesCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int):
        super().__init__(session, chunk_size)
        self._sp = get_spotipy()
        self._chunks_generator = DataChunksGenerator(chunk_size)

    async def collect(self) -> None:
        data = pd.read_csv(MERGED_DATA_PATH)
        data.drop_duplicates(subset=[NAME, ARTIST_NAME], inplace=True)
        artists_and_tracks = [(artist, track) for artist, track in zip(data[ARTIST_NAME], data[NAME])]
        chunks = self._chunks_generator.generate_data_chunks(
            lst=artists_and_tracks,
            filtering_list=self._get_existing_tracks_and_artists()
        )

        for chunk in chunks:
            await self._collect_single_chunk(chunk)

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
        now = get_current_datetime()
        output_path = AUDIO_FEATURES_CHUNK_OUTPUT_PATH_FORMAT.format(now)

        tracks_features_data.to_csv(output_path, encoding=UTF_8_ENCODING, index=False)

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
        query = f'artist:{artist} track:{track}'
        query_result = self._sp.search(q=query, type="track")
        track_uri = query_result[TRACKS][ITEMS][0][URI]
        split_uri = track_uri.split(':')

        return split_uri[-1]
