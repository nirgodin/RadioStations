import asyncio
from typing import Dict, List

import pandas as pd
from shazamio import Shazam

from consts.audio_features_consts import KEY
from consts.data_consts import ARTIST_ID, ARTISTS
from consts.path_consts import SHAZAM_TRACKS_IDS_PATH, SHAZAM_ARTISTS_IDS_PATH
from consts.shazam_consts import APPLE_MUSIC_ADAM_ID
from tools.data_chunks_generator import DataChunksGenerator
from utils.data_utils import extract_column_existing_values
from utils.file_utils import append_to_csv


class ShazamArtistsIDsFetcher:
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam
        self._chunks_generator = DataChunksGenerator(chunk_size=100, max_chunks_number=10)

    async def fetch(self) -> None:
        raw_keys = extract_column_existing_values(path=SHAZAM_TRACKS_IDS_PATH, column_name=KEY)
        await self._chunks_generator.execute_by_chunk_in_parallel(
            lst=[key for key in raw_keys if not pd.isna(key)],
            filtering_list=extract_column_existing_values(path=SHAZAM_ARTISTS_IDS_PATH, column_name=KEY),
            element_func=self._fetch_single_track_artist,
            chunk_func=self._fetch_single_chunk_artists,
            expected_type=dict
        )

    @staticmethod
    def _fetch_single_chunk_artists(valid_record: List[dict]) -> None:
        data = pd.DataFrame.from_records(valid_record)
        append_to_csv(data, SHAZAM_ARTISTS_IDS_PATH)

    async def _fetch_single_track_artist(self, key: float) -> Dict[int, str]:
        track = await self._shazam.track_about(int(key))
        artists = track.get(ARTISTS, [])

        if artists:
            first_artist = artists[0]
            artist_id = first_artist.get(APPLE_MUSIC_ADAM_ID, "")
        else:
            artist_id = ""

        return {
            KEY: key,
            ARTIST_ID: artist_id
        }


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ShazamArtistsIDsFetcher().fetch())
