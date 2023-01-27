import asyncio
import os.path
from functools import partial
from typing import List, Optional

import pandas as pd
from asyncio_pool import AioPool
from pandas import DataFrame
from shazamio import Shazam
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.audio_features_consts import KEY
from consts.data_consts import TOTAL
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import SHAZAM_TRACKS_IDS_PATH, SHAZAM_LISTENING_COUNT_PATH


class ShazamListeningCountFetcher:
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam

    async def fetch_tracks_listening_count(self, max_tracks: Optional[int] = 1000):
        tracks_ids = self._get_tracks_ids(max_tracks)
        listening_count_records = await self._fetch_listening_count_records(tracks_ids)
        listening_count_data = pd.DataFrame.from_records(listening_count_records)

        self._to_csv(listening_count_data)

    def _get_tracks_ids(self, max_tracks: int) -> List[int]:
        tracks_ids_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        tracks_ids_data.dropna(subset=[KEY], inplace=True)
        tracks_ids = tracks_ids_data[KEY].unique().tolist()
        non_existing_tracks_ids = self._remove_existing_tracks_ids(tracks_ids)

        if max_tracks is None:
            return non_existing_tracks_ids

        return non_existing_tracks_ids[:max_tracks]

    @staticmethod
    def _remove_existing_tracks_ids(tracks_ids: List[float]) -> List[int]:
        if not os.path.exists(SHAZAM_LISTENING_COUNT_PATH):
            return [int(track_id) for track_id in tracks_ids]

        listening_count_data = pd.read_csv(SHAZAM_LISTENING_COUNT_PATH)

        return [int(track_id) for track_id in tracks_ids if track_id not in listening_count_data[KEY]]

    async def _fetch_listening_count_records(self, tracks_ids: List[int]) -> List[dict]:
        number_of_tracks = len(tracks_ids)
        print(f'Starting to fetch {number_of_tracks} tracks listening counts using ShazamListeningCountFetcher')
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=number_of_tracks) as progress_bar:
            func = partial(self._fetch_single_track_listening_count, progress_bar)

            return await pool.map(func, tracks_ids)

    async def _fetch_single_track_listening_count(self, progress_bar: tqdm, track_id: int) -> dict:
        progress_bar.update(1)
        response = await self._shazam.listening_counter(track_id=track_id)

        if not isinstance(response, dict):
            return {KEY: track_id}

        return {
            KEY: track_id,
            TOTAL: response[TOTAL]
        }

    @staticmethod
    def _to_csv(listening_count_data: DataFrame) -> None:
        if os.path.exists(SHAZAM_LISTENING_COUNT_PATH):
            listening_count_data.to_csv(SHAZAM_LISTENING_COUNT_PATH, header=False, index=False, mode='a', encoding=UTF_8_ENCODING)
        else:
            listening_count_data.to_csv(SHAZAM_LISTENING_COUNT_PATH, index=False, encoding=UTF_8_ENCODING)


if __name__ == '__main__':
    fetcher = ShazamListeningCountFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.fetch_tracks_listening_count(max_tracks=5))
