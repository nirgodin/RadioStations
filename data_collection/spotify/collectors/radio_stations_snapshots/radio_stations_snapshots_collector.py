import asyncio
import os
from functools import partial
from typing import List, Tuple

import pandas as pd
from aiohttp import ClientSession
from async_lru import alru_cache
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import PLAYLIST_URL_FORMAT, AIO_POOL_SIZE, ARTISTS_URL_FORMAT
from consts.data_consts import TRACK, ARTISTS, ID
from consts.env_consts import RADIO_STATIONS_SNAPSHOTS_DRIVE_ID
from consts.path_consts import RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT
from consts.playlists_consts import STATIONS
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from data_collection.spotify.collectors.radio_stations_snapshots.artist import Artist
from data_collection.spotify.collectors.radio_stations_snapshots.playlist import Playlist
from data_collection.spotify.collectors.radio_stations_snapshots.station import Station
from data_collection.spotify.collectors.radio_stations_snapshots.track import Track
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.datetime_utils import get_current_datetime
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import to_csv
from utils.spotify_utils import build_spotify_headers


class RadioStationsSnapshotsCollector:
    def __init__(self, session: ClientSession):
        self._session = session

    async def collect(self) -> None:
        playlists = await self._get_stations_playlists()
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

    async def _get_stations_playlists(self) -> List[Playlist]:
        pool = AioPool(AIO_POOL_SIZE)
        iterable = list(STATIONS.items())
        progress_bar = tqdm(total=len(iterable))
        func = partial(self._get_single_playlist, progress_bar)

        return await pool.map(func, iterable)

    async def _get_single_playlist(self, progress_bar: tqdm, station_name_and_playlist_id: Tuple[str, str]) -> Playlist:
        station_name, playlist_id = station_name_and_playlist_id
        url = PLAYLIST_URL_FORMAT.format(playlist_id)

        async with self._session.get(url) as raw_response:
            response = await raw_response.json()
            progress_bar.update(1)

        return Playlist.from_spotify_response(station_name=station_name, playlist=response)

    async def _get_playlist_tracks(self, playlist: Playlist) -> List[Track]:
        artists_ids = self._get_artists_ids(playlist.tracks)
        pool = AioPool(AIO_POOL_SIZE)
        progress_bar = tqdm(total=len(artists_ids))
        func = partial(self._get_single_track_artist, progress_bar)
        artists = await pool.map(func, artists_ids)

        return self._serialize_tracks(playlist.tracks, artists)

    def _get_artists_ids(self, raw_tracks: List[dict]) -> List[str]:
        artists_ids = []

        for raw_track in raw_tracks:
            artist_id = self._get_single_artist_id(raw_track)
            artists_ids.append(artist_id)

        return artists_ids

    @staticmethod
    def _get_single_artist_id(track: dict) -> str:
        return track.get(TRACK, {}).get(ARTISTS, [])[0][ID]

    @alru_cache(maxsize=700)
    async def _get_single_track_artist(self, progress_bar: tqdm, artist_id: str) -> Artist:
        url = ARTISTS_URL_FORMAT.format(artist_id)

        async with self._session.get(url) as raw_response:
            response = await raw_response.json()
            progress_bar.update(1)

        return Artist.from_spotify_response(response)

    @staticmethod
    def _serialize_tracks(raw_tracks: List[dict], artists: List[Artist]) -> List[Track]:
        tracks = []

        for raw_track, artist in zip(raw_tracks, artists):
            track = Track.from_raw_track(raw_track, artist)
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
