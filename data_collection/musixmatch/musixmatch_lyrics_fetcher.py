import asyncio
import os
from functools import partial
from typing import List, Dict, Optional, Tuple, Iterator

from aiohttp import ClientSession
from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.musixmatch_consts import MUSIXMATCH_API_KEY, DAILY_REQUESTS_LIMIT, \
    MUSIXMATCH_HEADERS, MUSIXMATCH_LYRICS_URL_FORMAT, MUSIXMATCH_TRACK_ID, LYRICS, BODY, MESSAGE
from consts.path_consts import MUSIXMATCH_TRACK_IDS_PATH, MUSIXMATCH_TRACKS_LYRICS_PATH
from utils.file_utils import to_json, read_json, append_dict_to_json


class MusixmatchLyricsFetcher:
    def __init__(self, request_limit: int = DAILY_REQUESTS_LIMIT):
        self._api_key = os.environ[MUSIXMATCH_API_KEY]
        self._request_limit = request_limit

    async def fetch_tracks_lyrics(self) -> None:
        tracks_ids = list(self._track_ids.keys())
        existing_tracks_lyrics = list(self._tracks_lyrics.keys())
        tracks_without_lyrics = [track_id for track_id in tracks_ids if track_id not in existing_tracks_lyrics]
        daily_subset = tracks_without_lyrics[:self._request_limit]

        await self._fetch_daily_subset(daily_subset)

    async def _fetch_daily_subset(self, spotify_track_ids: List[str]) -> None:
        if not spotify_track_ids:
            print('Did not find any relevant track id. Quiting job.')
            return

        valid_responses = await self._fetch_tracks_lyrics(spotify_track_ids)
        append_dict_to_json(
            existing_data=self._tracks_lyrics,
            new_data=valid_responses,
            path=MUSIXMATCH_TRACKS_LYRICS_PATH
        )

    async def _fetch_tracks_lyrics(self, spotify_track_ids: List[str]) -> Dict[str, dict]:
        musixmatch_track_ids = map(lambda track_id: self._map_spotify_to_musixmatch_track_id(track_id), spotify_track_ids)
        raw_responses = await self._fetch_raw_responses(list(spotify_track_ids))
        zipped_responses_and_ids = zip(raw_responses, spotify_track_ids, musixmatch_track_ids)

        return self._get_valid_responses(zipped_responses_and_ids)

    async def _fetch_raw_responses(self, spotify_track_ids: List[str]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)

        async with ClientSession(headers=MUSIXMATCH_HEADERS) as session:
            with tqdm(total=len(spotify_track_ids)) as progress_bar:
                func = partial(self._get_single_track_lyrics, progress_bar, session)

                return await pool.map(fn=func, iterable=spotify_track_ids)

    async def _get_single_track_lyrics(self,
                                       progress_bar: tqdm,
                                       session: ClientSession,
                                       spotify_track_id: str) -> dict:
        progress_bar.update(1)
        musixmatch_track_id = self._map_spotify_to_musixmatch_track_id(spotify_track_id)

        if not musixmatch_track_id:
            return {}

        url = self._build_request_url(musixmatch_track_id)

        async with session.get(url=url) as response:
            track_lyrics_response = await response.json(content_type=None)

        return track_lyrics_response

    def _map_spotify_to_musixmatch_track_id(self, spotify_track_id: str) -> str:
        return self._track_ids.get(spotify_track_id, {}).get(MUSIXMATCH_TRACK_ID, '')

    def _build_request_url(self, track_id: str) -> str:
        return MUSIXMATCH_LYRICS_URL_FORMAT.format(track_id, self._api_key)

    def _get_valid_responses(self, zipped_responses_and_ids: Iterator[Tuple[dict, str, str]]) -> Dict[str, dict]:
        valid_responses = {}

        for response, spotify_track_id, musixmatch_track_id in zipped_responses_and_ids:
            try:
                response_lyrics = self._extract_single_response_lyrics(response, musixmatch_track_id)
            except Exception as e:
                response_lyrics = None

            if response_lyrics is not None:
                valid_responses[spotify_track_id] = response_lyrics
            else:
                valid_responses[spotify_track_id] = {}

        return valid_responses

    def _extract_single_response_lyrics(self, response: dict, musixmatch_track_id: str) -> Optional[dict]:
        if not isinstance(response, dict):
            return

        response_lyrics = self._extract_lyrics_from_response(response)

        if response_lyrics is not None:
            response_lyrics[MUSIXMATCH_TRACK_ID] = musixmatch_track_id

            return response_lyrics

    @staticmethod
    def _extract_lyrics_from_response(response: dict) -> Optional[dict]:
        body = response.get(MESSAGE, {}).get(BODY, {})

        if isinstance(body, list):
            return

        return body.get(LYRICS, None)

    @property
    def _track_ids(self) -> Dict[str, dict]:
        if not os.path.exists(MUSIXMATCH_TRACK_IDS_PATH):
            return {}

        return read_json(path=MUSIXMATCH_TRACK_IDS_PATH)

    @property
    def _tracks_lyrics(self) -> Dict[str, dict]:
        if not os.path.exists(MUSIXMATCH_TRACKS_LYRICS_PATH):
            return {}

        return read_json(path=MUSIXMATCH_TRACKS_LYRICS_PATH)


if __name__ == '__main__':
    fetcher = MusixmatchLyricsFetcher(request_limit=1)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.fetch_tracks_lyrics())
