import asyncio
import os
from typing import List, Optional

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame

from consts.api_consts import AIO_POOL_SIZE, ARTISTS_URL_FORMAT
from consts.data_consts import TRACK, ARTISTS, ID
from consts.env_consts import RADIO_STATIONS_SNAPSHOTS_DRIVE_ID
from consts.path_consts import RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT
from consts.playlists_consts import STATIONS
from data_collection.spotify.collectors.playlists_collector import PlaylistsCollector
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.artist import Artist
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.playlist import Playlist
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.station import Station
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.track import Track
from tools.data_chunks_generator import DataChunksGenerator
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.datetime_utils import get_current_datetime
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import to_csv
from utils.general_utils import chain_lists
from utils.spotify_utils import build_spotify_headers

MAX_ARTISTS_PER_REQUEST = 50


class RadioStationsSnapshotsCollector:
    def __init__(self, session: ClientSession):
        self._session = session
        self._playlists_collector = PlaylistsCollector(self._session)

    async def collect(self) -> None:
        print('Starting to run `RadioStationsSnapshotsCollector`')
        playlists = await self._playlists_collector.collect(STATIONS)
        stations = await self._collect_stations(playlists)
        dfs = [station.to_dataframe() for station in stations]
        data = pd.concat(dfs)

        self._export_results(data)

    async def _collect_stations(self, playlists: List[Playlist]) -> List[Station]:
        stations = []

        for playlist in playlists:
            playlist_tracks = await self._get_playlist_tracks(playlist)
            station = Station(playlist=playlist, tracks=playlist_tracks)
            stations.append(station)

        return stations

    async def _get_playlist_tracks(self, playlist: Playlist) -> List[Track]:
        print(f'Starting to collect `{playlist.station}` station artists')
        artists_ids = self._get_artists_ids(playlist.tracks)
        chunks_generator = DataChunksGenerator(chunk_size=MAX_ARTISTS_PER_REQUEST, max_chunks_number=None)
        chunks = chunks_generator.generate_data_chunks(lst=artists_ids, filtering_list=None)
        pool = AioPool(AIO_POOL_SIZE)
        artists = await pool.map(self._get_single_chunk_artists, chunks)
        flattened_artists = chain_lists(artists)

        return self._serialize_tracks(playlist.tracks, flattened_artists)

    def _get_artists_ids(self, raw_tracks: List[dict]) -> List[str]:
        artists_ids = []

        for raw_track in raw_tracks:
            artist_id = self._get_single_artist_id(raw_track)

            if artist_id is not None:
                artists_ids.append(artist_id)

        return artists_ids

    @staticmethod
    def _get_single_artist_id(track: dict) -> Optional[str]:
        inner_track = track.get(TRACK, {})
        if inner_track is None:
            return

        artists = inner_track.get(ARTISTS, [])
        if not artists:
            return

        return artists[0][ID]

    async def _get_single_chunk_artists(self, chunk: List[str]) -> List[Artist]:
        artists_ids = ','.join(chunk)
        url = ARTISTS_URL_FORMAT.format(artists_ids)

        async with self._session.get(url) as raw_response:
            response = await raw_response.json()

        artists = response[ARTISTS]
        return [Artist.from_spotify_response(artist) for artist in artists]

    @staticmethod
    def _serialize_tracks(raw_tracks: List[dict], artists: List[Artist]) -> List[Track]:
        tracks = []

        for raw_track, artist in zip(raw_tracks, artists):
            inner_track = raw_track.get(TRACK, {})

            if inner_track is not None:
                track = Track.from_raw_track(inner_track, raw_track, artist)
                tracks.append(track)

        return tracks

    @staticmethod
    def _export_results(data: DataFrame) -> None:
        now = get_current_datetime()
        output_path = RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT.format(now)
        to_csv(data=data, output_path=output_path)
        file_metadata = GoogleDriveUploadMetadata(
            local_path=output_path,
            drive_folder_id=os.environ[RADIO_STATIONS_SNAPSHOTS_DRIVE_ID]
        )
        upload_files_to_drive(file_metadata)


if __name__ == '__main__':
    session = ClientSession(headers=build_spotify_headers())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(RadioStationsSnapshotsCollector(session).collect())
