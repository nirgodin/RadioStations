import asyncio
import os.path
from functools import partial
from typing import Dict, List

import pandas as pd
from asyncio_pool import AioPool
from pandas import DataFrame
from shazamio import Shazam
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import ID, SONG, SPOTIFY_ID
from consts.path_consts import MERGED_DATA_PATH, SHAZAM_TRACKS_IDS_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY, HITS, TRACKS, TITLE, HEADING, SUBTITLE, APPLE_MUSIC_ADAM_ID, ARTISTS
from utils import append_to_csv


class ShazamSearchFetcher:
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam

    async def search_tracks(self, data: DataFrame, max_tracks: int = 1000) -> None:
        relevant_data = self._extract_relevant_data(data)
        run_subset = relevant_data.head(max_tracks)
        tracks_ids_records = await self._search(run_subset)
        tracks_ids_data = pd.DataFrame.from_records(tracks_ids_records)
        merged_spotify_ids_data = self._merge_tracks_and_spotify_ids_data(tracks_ids_data, run_subset)

        append_to_csv(data=merged_spotify_ids_data, output_path=SHAZAM_TRACKS_IDS_PATH)

    @staticmethod
    def _extract_relevant_data(data: DataFrame):
        non_na_data = data.dropna(subset=[ID, SONG])
        non_duplicated_data = non_na_data.drop_duplicates(subset=[ID, SONG])

        if not os.path.exists(SHAZAM_TRACKS_IDS_PATH):
            return non_duplicated_data

        existing_tracks_ids_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        exiting_song_names = existing_tracks_ids_data[SONG].tolist()

        return non_duplicated_data[~non_duplicated_data[SONG].isin(exiting_song_names)]

    async def _search(self, data: DataFrame) -> List[Dict[str, str]]:
        number_of_tracks = len(data)
        print(f'Starting to fetch {number_of_tracks} tracks ids using ShazamSearchFetcher')
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=number_of_tracks) as progress_bar:
            func = partial(self._search_single_track, progress_bar)

            return await pool.map(func, data[SONG].tolist())

    async def _search_single_track(self, progress_bar: tqdm, song: str) -> Dict[str, str]:
        progress_bar.update(1)
        response = await self._shazam.search_track(query=song, limit=1)

        if not isinstance(response, dict):
            return {SONG: song}

        return self._extract_relevant_info(song, response)

    def _extract_relevant_info(self, song: str, response: dict) -> Dict[str, str]:
        hits = response.get(TRACKS, {}).get(HITS, [])

        if not hits:
            return {SONG: song}

        first_hit = hits[0]

        return {
            SONG: song,
            SHAZAM_TRACK_KEY: first_hit.get(SHAZAM_TRACK_KEY, ''),
            TITLE: self._extract_track_title(first_hit, TITLE),
            SUBTITLE: self._extract_track_title(first_hit, SUBTITLE),
            APPLE_MUSIC_ADAM_ID: self._extract_adam_id(first_hit)
        }

    @staticmethod
    def _extract_track_title(first_hit: dict, field: str) -> str:
        return first_hit.get(HEADING, {}).get(field, '')

    @staticmethod
    def _extract_adam_id(first_hit: dict) -> str:
        artists = first_hit.get(ARTISTS, [])

        if not artists:
            return ''

        first_artist = artists[0]

        return first_artist.get(APPLE_MUSIC_ADAM_ID, '')

    @staticmethod
    def _merge_tracks_and_spotify_ids_data(tracks_ids_data: DataFrame, spotify_data: DataFrame):
        spotify_relevant_columns = spotify_data[[ID, SONG]]
        spotify_relevant_columns.rename(columns={ID: SPOTIFY_ID}, inplace=True)

        return tracks_ids_data.merge(
            right=spotify_relevant_columns,
            how='left',
            on=[SONG]
        )


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH)
    fetcher = ShazamSearchFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.search_tracks(data))
