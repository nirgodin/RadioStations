import asyncio
import os.path
from functools import partial
from typing import List, Union, Dict, Iterator

import pandas as pd
from asyncio_pool import AioPool
from shazamio import Shazam
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import TYPE
from consts.path_consts import SHAZAM_TRACKS_ABOUT_PATH, SHAZAM_TRACKS_IDS_PATH, SHAZAM_TRACKS_LYRICS_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY, TITLE, SUBTITLE, GENRES, PRIMARY, PRIMARY_GENRE, ALBUM_ADAM_ID, \
    TRACK_ADAM_ID, SHAZAM_RELEASE_DATE, SECTIONS, METADATA, TEXT, ALBUM, LABEL, SHAZAM_LYRICS, BEACON_DATA, LYRICS_ID, \
    LYRICS_PROVIDER_NAME, LYRICS_PROVIDER_TRACK_ID, COMMON_TRACK_ID, LYRICS_TEXT, LYRICS_FOOTER
from utils.data_utils import extract_column_existing_values
from utils.file_utils import read_json, to_json, append_to_csv, append_dict_to_json


class ShazamTrackAboutFetcher:
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam
        self._existing_tracks = extract_column_existing_values(SHAZAM_TRACKS_ABOUT_PATH, SHAZAM_TRACK_KEY)

    async def fetch_tracks_about(self, max_tracks: int) -> None:
        records = await self._fetch_records(max_tracks)
        data = pd.DataFrame.from_records(records)

        non_lyrics_data = data.drop(LYRICS_TEXT, axis=1)
        append_to_csv(data=non_lyrics_data, output_path=SHAZAM_TRACKS_ABOUT_PATH, escapechar=r"|")

        lyrics_data = {str(track[SHAZAM_TRACK_KEY]): track[LYRICS_TEXT] for track in records}
        self._to_json(lyrics_data)

    async def _fetch_records(self, max_tracks: int) -> List[dict]:
        relevant_tracks_ids = self._get_relevant_tracks_ids(max_tracks)
        print('Starting to fetch tracks about records')
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(relevant_tracks_ids)) as progress_bar:
            func = partial(self._fetch_single_track_info, progress_bar)
            raw_records = await pool.map(func, relevant_tracks_ids)

        return [record for record in raw_records if isinstance(record, dict)]

    def _get_relevant_tracks_ids(self, max_tracks: int) -> List[int]:
        print('Starting to extract relevant tracks ids')
        relevant_tracks_ids = []

        with tqdm(total=max_tracks) as progress_bar:
            for track_id in self._generate_tracks_ids():
                if self._is_relevant_track(track_id):
                    relevant_tracks_ids.append(int(track_id))
                    progress_bar.update(1)

                if len(relevant_tracks_ids) == max_tracks:
                    break

        return relevant_tracks_ids

    @staticmethod
    def _generate_tracks_ids() -> Iterator:
        tracks_ids_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        unique_tracks_ids = tracks_ids_data[SHAZAM_TRACK_KEY].unique().tolist()

        return reversed(unique_tracks_ids)

    def _is_relevant_track(self, track_id: float) -> bool:
        if pd.isna(track_id):
            return False

        return track_id not in self._existing_tracks

    async def _fetch_single_track_info(self, progress_bar: tqdm, track_id: int) -> dict:
        progress_bar.update(1)
        response = await self._shazam.track_about(track_id=track_id)

        return {
            SHAZAM_TRACK_KEY: track_id,
            TITLE: response.get(TITLE, ''),
            SUBTITLE: response.get(SUBTITLE, ''),
            PRIMARY_GENRE: self._extract_primary_genre(response),
            ALBUM_ADAM_ID: response.get(ALBUM_ADAM_ID, ''),
            TRACK_ADAM_ID: response.get(TRACK_ADAM_ID, ''),
            SHAZAM_RELEASE_DATE: response.get(SHAZAM_RELEASE_DATE, ''),
            ALBUM: self._extract_metadata_item(response, ALBUM),
            LABEL: self._extract_metadata_item(response, LABEL),
            LYRICS_ID: self._extract_lyrics_beacon_data(response, LYRICS_ID),
            LYRICS_PROVIDER_NAME: self._extract_lyrics_beacon_data(response, LYRICS_PROVIDER_NAME),
            LYRICS_PROVIDER_TRACK_ID: self._extract_lyrics_beacon_data(response, COMMON_TRACK_ID),
            LYRICS_TEXT: self._extract_lyrics_data(response, TEXT, []),
            LYRICS_FOOTER: self._extract_lyrics_data(response, LYRICS_FOOTER, '')
        }

    @staticmethod
    def _extract_primary_genre(response: dict) -> str:
        return response.get(GENRES, {}).get(PRIMARY, '')

    @staticmethod
    def _extract_metadata_item(response: dict, title: str) -> str:
        for section in response.get(SECTIONS, []):
            for item in section.get(METADATA, []):
                item_title = item.get(TITLE, '')

                if item_title == title:
                    return item.get(TEXT, '')

        return ''

    def _extract_lyrics_beacon_data(self, response: dict, key: str) -> str:
        shazam_lyrics_section = self._extract_shazam_lyrics_section(response)
        return shazam_lyrics_section.get(BEACON_DATA, {}).get(key, '')

    def _extract_lyrics_data(self, response: dict, key: str, default_value: Union[str, list]) -> Union[str, List[str]]:
        shazam_lyrics_section = self._extract_shazam_lyrics_section(response)
        return shazam_lyrics_section.get(key, default_value)

    @staticmethod
    def _extract_shazam_lyrics_section(response: dict) -> dict:
        for section in response.get(SECTIONS, []):
            section_type = section.get(TYPE, '')

            if section_type == SHAZAM_LYRICS:
                return section

        return {}

    @staticmethod
    def _to_json(tracks_lyrics: Dict[str, List[str]]) -> None:
        if not os.path.exists(SHAZAM_TRACKS_LYRICS_PATH):
            to_json(d=tracks_lyrics, path=SHAZAM_TRACKS_LYRICS_PATH)

        append_dict_to_json(
            existing_data=read_json(SHAZAM_TRACKS_LYRICS_PATH),
            new_data=tracks_lyrics,
            path=SHAZAM_TRACKS_LYRICS_PATH
        )


if __name__ == '__main__':
    fetcher = ShazamTrackAboutFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.fetch_tracks_about(max_tracks=200))
