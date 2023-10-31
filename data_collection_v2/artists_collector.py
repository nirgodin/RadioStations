from typing import List

from postgres_client.models.orm.spotify_artist import SpotifyArtist
from sqlalchemy.ext.asyncio import AsyncEngine
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
from consts.playlists_consts import STATIONS, GLGLZ
from data_collection.spotify.collectors.playlists_collector import PlaylistsCollector
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.artist import Artist
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.playlist import Playlist
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.station import Station
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.track import Track
from tools.data_chunks_generator import DataChunksGenerator
from tools.environment_manager import EnvironmentManager
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.datetime_utils import get_current_datetime
from utils.drive_utils import upload_files_to_drive
from utils.file_utils import to_csv
from utils.general_utils import chain_lists
from utils.spotify_utils import build_spotify_headers

MAX_ARTISTS_PER_REQUEST = 50


class ArtistsCollector:
    def __init__(self,
                 session: ClientSession,
                 db_engine: AsyncEngine,
                 chunks_generator: DataChunksGenerator):
        self._session = session
        self._db_engine = db_engine
        self._chunks_generator = chunks_generator

    async def collect(self, tracks: List[dict]) -> List[SpotifyArtist]:
        artists_ids = self._get_artists_ids(tracks)
        artists = await self._chunks_generator.execute_by_chunk_in_parallel(
            lst=artists_ids,
            filtering_list=[],
            func=self._get_single_chunk_artists
        )
        flattened_artists = chain_lists(artists)

    def _get_artists_ids(self, tracks: List[dict]) -> List[str]:
        artists_ids = []

        for track in tracks:
            artist_id = self._get_single_artist_id(track)

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
        return [SpotifyArtist.from_spotify_response(artist) for artist in artists]
