import asyncio
from typing import List

from aiohttp import ClientSession
from postgres_client.postgres_operations import get_database_engine
from spotipyio.logic.spotify_client import SpotifyClient

from consts.data_consts import ID, TRACKS, ITEMS
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.station import Station
from data_collection_v2.database_insertion.albums_database_inserter import AlbumsDatabaseInserter
from data_collection_v2.database_insertion.artists_database_inserter import ArtistsDatabaseInserter
from data_collection_v2.database_insertion.audio_features_database_inserter import AudioFeaturesDatabaseInserter
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter
from data_collection_v2.database_insertion.tracks_database_inserter import TracksDatabaseInserter
from tools.environment_manager import EnvironmentManager
from tools.logging import logger
from utils.spotify_utils import build_spotify_headers


class RadioStationsSnapshotsCollector:
    def __init__(self,
                 spotify_client: SpotifyClient,
                 artists_database_inserter: ArtistsDatabaseInserter,
                 albums_database_inserter: AlbumsDatabaseInserter,
                 tracks_database_inserter: TracksDatabaseInserter,
                 audio_features_database_inserter: AudioFeaturesDatabaseInserter):
        self._spotify_client = spotify_client
        self._artists_database_inserter = artists_database_inserter
        self._albums_database_inserter = albums_database_inserter
        self._tracks_database_inserter = tracks_database_inserter
        self._audio_features_database_inserter = audio_features_database_inserter

    async def collect(self) -> None:
        logger.info('Starting to run `RadioStationsSnapshotsCollector`')
        playlists = await self._spotify_client.playlists.collect(['18cUFeM5Q75ViwevsMQM1j'])
        await self._insert_records_to_db(playlists)
        logger.info('Successfully collected and inserted playlists to DB')

    async def _insert_records_to_db(self, playlists: List[dict]) -> List[Station]:
        for playlist in playlists:
            await self._insert_spotify_records(playlist)
            await self._insert_radio_tracks_records()

    async def _insert_spotify_records(self, playlist: dict) -> None:
        print(f'Starting to insert playlist `{playlist[ID]}` spotify records')
        tracks = playlist[TRACKS][ITEMS]

        for database_inserter in self._ordered_database_inserters:
            await database_inserter.insert(tracks)

    async def _insert_radio_tracks_records(self, playlist: dict):  # TODO: Complete
        pass

    @property
    def _ordered_database_inserters(self) -> List[BaseDatabaseInserter]:
        return [
            self._artists_database_inserter,
            self._albums_database_inserter,
            self._tracks_database_inserter,
            self._audio_features_database_inserter
        ]


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    session = ClientSession(headers=build_spotify_headers())
    db_engine = get_database_engine()
    spotify_client = SpotifyClient.create(session)
    snapshots_collector = RadioStationsSnapshotsCollector(
        spotify_client=spotify_client,
        artists_database_inserter=ArtistsDatabaseInserter(db_engine, spotify_client),
        albums_database_inserter=AlbumsDatabaseInserter(db_engine),
        tracks_database_inserter=TracksDatabaseInserter(db_engine),
        audio_features_database_inserter=AudioFeaturesDatabaseInserter(db_engine, spotify_client)
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(snapshots_collector.collect())
