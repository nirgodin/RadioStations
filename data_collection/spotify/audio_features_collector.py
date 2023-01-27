import asyncio
from functools import partial
from typing import Union, Dict, Tuple, List

import numpy as np
import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.miscellaneous_consts import UTF_8_ENCODING
from data_collection.spotify.access_token_generator import AccessTokenGenerator
from consts.api_consts import AUDIO_FEATURES_URL_FORMAT, AIO_POOL_SIZE
from consts.data_consts import NAME, ARTIST_NAME, TRACKS, ITEMS, URI
from consts.path_consts import MERGED_DATA_PATH
from utils import get_current_datetime, get_spotipy


class AudioFeaturesCollector:
    def __init__(self):
        self._sp = get_spotipy()
        self._session = ClientSession(headers=self._build_headers())

    async def collect(self, data: DataFrame) -> None:
        unique_tracks_data = data.drop_duplicates(subset=[NAME, ARTIST_NAME])
        chunks_number = round(len(unique_tracks_data) / 1000)
        chunks = np.array_split(unique_tracks_data, chunks_number)

        for i, chunk in enumerate(chunks):
            print(f'Starting to process chunk {i+1} out of {chunks_number}')
            await self._collect_single_chunk(chunk)

    async def _collect_single_chunk(self, chunk: DataFrame) -> None:
        tracks_features = await self._get_tracks_features(chunk)
        valid_features = [feature for feature in tracks_features if isinstance(feature, dict)]
        print(f'Failed to collect audio features for {len(tracks_features) - len(valid_features)} out of {len(tracks_features)} tracks')
        tracks_features_data = pd.DataFrame.from_records(valid_features)
        now = get_current_datetime()
        tracks_features_data.to_csv(fr'data/audio_features/{now}.csv', encoding=UTF_8_ENCODING, index=False)

    async def _get_tracks_features(self, data: DataFrame) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)
        iterable = [(artist, track) for artist, track in zip(data[ARTIST_NAME], data[NAME])]

        with tqdm(total=len(data)) as progress_bar:
            func = partial(self._get_single_track_features, progress_bar)

            return await pool.map(fn=func, iterable=iterable)

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

    @staticmethod
    def _is_access_token_expired(response: dict) -> bool:
        return response.get('error', {}).get('status') == 401

    async def _renew_client_session(self) -> None:
        await self._session.close()
        self._session = ClientSession(headers=self._build_headers())

    @staticmethod
    def _build_headers() -> Dict[str, str]:
        bearer_token = AccessTokenGenerator.generate()
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}"
        }


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(AudioFeaturesCollector().collect(data))
