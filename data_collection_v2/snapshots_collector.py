import asyncio
import os
from typing import List

import pandas as pd
from aiohttp import ClientSession
from pandas import DataFrame
from postgres_client.postgres_operations import get_database_engine

from consts.data_consts import TRACK
from consts.env_consts import RADIO_STATIONS_SNAPSHOTS_DRIVE_ID
from consts.path_consts import RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT
from consts.playlists_consts import GLGLZ
from data_collection.spotify.collectors.playlists_collector import PlaylistsCollector
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.artist import Artist
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.playlist import Playlist
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.station import Station
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.track import Track
from data_collection_v2.albums_database_inserter import AlbumsDatabaseInserter
from data_collection_v2.artists_database_inserter import ArtistsDatabaseInserter
from tools.environment_manager import EnvironmentManager
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from tools.logging import logger
from utils.datetime_utils import get_current_datetime
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import to_csv
from utils.spotify_utils import build_spotify_headers


class RadioStationsSnapshotsCollector:
    def __init__(self,
                 playlists_collector: PlaylistsCollector,
                 artists_database_inserter: ArtistsDatabaseInserter,
                 albums_database_inserter: AlbumsDatabaseInserter):
        self._playlists_collector = playlists_collector
        self._artists_database_inserter = artists_database_inserter
        self._albums_database_inserter = albums_database_inserter

    async def collect(self) -> None:
        logger.info('Starting to run `RadioStationsSnapshotsCollector`')
        playlists = await self._playlists_collector.collect({GLGLZ: '18cUFeM5Q75ViwevsMQM1j'})  # TODO: Remove
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
        artists = []
        # artists = await self._artists_collector.collect(playlist.tracks)

        return self._serialize_tracks(playlist.tracks, artists)

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
    EnvironmentManager().set_env_variables()
    session = ClientSession(headers=build_spotify_headers())
    playlists_collector = PlaylistsCollector(session)
    db_engine = get_database_engine()
    spotify_client = "mock"
    artists_collector = ArtistsCollector(db_engine, spotify_client)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(RadioStationsSnapshotsCollector(playlists_collector, artists_collector).collect())
