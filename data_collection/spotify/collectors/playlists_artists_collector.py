import asyncio
import os.path
from typing import List, Optional, Dict, Generator

import pandas as pd
from aiohttp import ClientSession
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import TRACK, ARTISTS, ARTIST_ID, ID, ARTIST_NAME, NAME, STATION
from consts.miscellaneous_consts import NAMED_PLAYLISTS, OUTPUT_PATH, RECORD_KEY, RECORD_VALUE
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from data_collection.spotify.collectors.playlists_collector import PlaylistsCollector
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.playlist import Playlist
from tools.environment_manager import EnvironmentManager
from utils.file_utils import append_to_csv
from utils.general_utils import chain_lists, binary_search
from utils.spotify_utils import build_spotify_headers


class PlaylistsArtistsCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        super().__init__(session, chunk_size, max_chunks_number)
        self._session = session
        self._playlists_collector = PlaylistsCollector(self._session)

    async def collect(self, **kwargs) -> None:
        EnvironmentManager().set_env_variables()
        playlists = await self._playlists_collector.collect(kwargs[NAMED_PLAYLISTS])
        artists = self._generate_playlists_main_artists(
            playlists=playlists,
            record_key=kwargs[RECORD_KEY],
            record_value=kwargs[RECORD_VALUE]
        )
        records = chain_lists(artists)
        output_path = kwargs[OUTPUT_PATH]
        data = self._add_non_existing_artists(records, output_path)

        append_to_csv(data, output_path)

    def _generate_playlists_main_artists(self,
                                         playlists: List[Playlist],
                                         record_key: str,
                                         record_value: str) -> Generator[List[Dict[str, str]], None, None]:
        for playlist in playlists:
            print(f'Starting to extract `{playlist.station}` artists')

            yield self._extract_single_playlist_main_artists(playlist, record_key, record_value)

    def _extract_single_playlist_main_artists(self,
                                              playlist: Playlist,
                                              record_key: str,
                                              record_value: str) -> List[Dict[str, str]]:
        records = []

        with tqdm(total=len(playlist.tracks)) as progress_bar:
            for track in playlist.tracks:
                record = self._extract_single_track_main_artist(track, playlist.station, record_key, record_value)
                records.append(record)
                progress_bar.update(1)

        return records

    def _extract_single_track_main_artist(self,
                                          track: dict,
                                          playlist_name: str,
                                          record_key: str,
                                          record_value: str) -> Optional[Dict[str, str]]:
        artists = self._extract_raw_track_artists(track)
        if not artists:
            return

        main_artist = artists[0]
        return {
            ARTIST_ID: main_artist[ID],
            ARTIST_NAME: main_artist[NAME],
            record_key: record_value,
            STATION: playlist_name
        }

    @staticmethod
    def _extract_raw_track_artists(track: dict) -> Optional[list]:
        inner_track = track.get(TRACK, {})

        if inner_track is None:
            return

        return track.get(TRACK, {}).get(ARTISTS, [])

    def _add_non_existing_artists(self, new_records: List[Dict[str, str]], output_path: str) -> DataFrame:
        print('Starting to add non existing artists')
        records = self._get_existing_artists_data(output_path)
        existing_artists = [record[ARTIST_NAME] for record in records]

        with tqdm(total=len(records)) as progress_bar:
            for record in new_records:
                self._add_single_record(record, records, existing_artists)
                progress_bar.update(1)

        return pd.DataFrame.from_records(records)

    @staticmethod
    def _get_existing_artists_data(output_path: str) -> List[Dict[str, str]]:
        if not os.path.exists(output_path):
            return []

        data = pd.read_csv(output_path)
        data.dropna(subset=[ARTIST_NAME], inplace=True)

        return data.to_dict(orient='records')

    @staticmethod
    def _add_single_record(record: Dict[str, str], records: List[Dict[str, str]], existing_artists: List[str]) -> None:
        artist_name = record[ARTIST_NAME]
        is_artist_in_list, insert_index = binary_search(lst=existing_artists, value=artist_name)

        if not is_artist_in_list:
            records.insert(insert_index, record)
            existing_artists.insert(insert_index, artist_name)

    async def _collect_single_chunk(self, chunk: list) -> None:
        pass


if __name__ == '__main__':
    client_session = ClientSession(headers=build_spotify_headers())
    loop = asyncio.get_event_loop()
    collector = PlaylistsArtistsCollector(client_session, 5, 5)
    loop.run_until_complete(collector.collect())
