import asyncio
import json
import os
from functools import partial
from typing import Tuple, List, Dict

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from api.musixmatch.track_serach_response_reader import TrackSearchResponseReader
from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ARTIST_NAME, NAME, ID
from consts.musixmatch_consts import MUSIXMATCH_TRACK_SEARCH_URL_FORMAT, MUSIXMATCH_API_KEY, DAILY_REQUESTS_LIMIT, \
    MUSIXMATCH_HEADERS
from consts.path_consts import MUSIXMATCH_TRACK_IDS_PATH, MERGED_DATA_PATH

MUSIXMATCH_RELEVANT_COLUMNS = [
    ARTIST_NAME,
    NAME,
    ID
]


class MusixmatchSearchFetcher:
    def __init__(self,
                 request_limit: int = DAILY_REQUESTS_LIMIT,
                 response_reader: TrackSearchResponseReader = TrackSearchResponseReader()):
        self._request_limit = request_limit
        self._api_key = os.environ[MUSIXMATCH_API_KEY]
        self._response_reader = response_reader

    async def fetch_tracks_ids(self, data: DataFrame) -> None:
        non_existing_tracks_data = data[~data[ID].isin(self._existing_tracks.keys())]
        relevant_data = non_existing_tracks_data[MUSIXMATCH_RELEVANT_COLUMNS]
        daily_subset = relevant_data.head(self._request_limit).reset_index(drop=True)
        valid_responses = await self._fetch_tracks(daily_subset)
        valid_responses.update(self._existing_tracks)

        with open(MUSIXMATCH_TRACK_IDS_PATH, 'w') as f:
            json.dump(valid_responses, f, indent=4)

    async def _fetch_tracks(self, data: DataFrame) -> Dict[str, dict]:
        raw_responses = await self._fetch_raw_responses(data)
        valid_responses = {}

        for response, track_id in zip(raw_responses, data[ID]):
            if isinstance(response, dict):
                formatted_response = self._response_reader.read(response)

                if formatted_response is not None:
                    valid_responses[track_id] = formatted_response

        return valid_responses

    async def _fetch_raw_responses(self, data: DataFrame) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)
        iterable = [(artist, track) for artist, track in zip(data[ARTIST_NAME], data[NAME])]

        async with ClientSession(headers=MUSIXMATCH_HEADERS) as session:
            with tqdm(total=len(data)) as progress_bar:
                func = partial(self._get_single_track_id, progress_bar, session)

                return await pool.map(fn=func, iterable=iterable)

    async def _get_single_track_id(self,
                                   progress_bar: tqdm,
                                   session: ClientSession,
                                   artist_and_track: Tuple[str, str]) -> dict:
        progress_bar.update(1)
        artist, track = artist_and_track
        url = self._build_request_url(artist, track)

        async with session.get(url=url) as response:
            track_search_response = await response.json(content_type=None)

        return track_search_response

    def _build_request_url(self, artist: str, track: str) -> str:
        return MUSIXMATCH_TRACK_SEARCH_URL_FORMAT.format(artist, track, self._api_key)

    @property
    def _existing_tracks(self) -> Dict[str, dict]:
        if not os.path.exists(MUSIXMATCH_TRACK_IDS_PATH):
            return {}

        with open(MUSIXMATCH_TRACK_IDS_PATH, 'r') as f:
            return json.load(f)


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH)
    fetcher = MusixmatchSearchFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.fetch_tracks_ids(data))
