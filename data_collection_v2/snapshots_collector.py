import asyncio
from typing import List, Dict

from aiohttp import ClientSession
from postgres_client import BaseSpotifyORMModel
from postgres_client.postgres_operations import get_database_engine
from spotipyio.logic.spotify_client import SpotifyClient

from consts.data_consts import ID, TRACKS, ITEMS, ARTISTS
from data_collection_v2.database_insertion.radio_tracks_database_inserter import RadioTracksDatabaseInserter
from data_collection_v2.database_insertion.spotify_database_inserters.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter
from data_collection_v2.database_insertion.spotify_database_inserters.spotify_albums_database_inserter import \
    SpotifyAlbumsDatabaseInserter
from data_collection_v2.database_insertion.spotify_database_inserters.spotify_artists_database_inserter import \
    SpotifyArtistsDatabaseInserter
from data_collection_v2.database_insertion.spotify_database_inserters.spotify_audio_features_database_inserter import \
    SpotifyAudioFeaturesDatabaseInserter
from data_collection_v2.database_insertion.spotify_database_inserters.spotify_tracks_database_inserter import \
    SpotifyTracksDatabaseInserter
from tools.environment_manager import EnvironmentManager
from tools.logging import logger
from utils.spotify_utils import build_spotify_headers


class RadioStationsSnapshotsCollector:
    def __init__(self,
                 spotify_client: SpotifyClient,
                 artists_database_inserter: SpotifyArtistsDatabaseInserter,
                 albums_database_inserter: SpotifyAlbumsDatabaseInserter,
                 tracks_database_inserter: SpotifyTracksDatabaseInserter,
                 audio_features_database_inserter: SpotifyAudioFeaturesDatabaseInserter,
                 radio_tracks_database_inserter: RadioTracksDatabaseInserter):
        self._spotify_client = spotify_client
        self._artists_database_inserter = artists_database_inserter
        self._albums_database_inserter = albums_database_inserter
        self._tracks_database_inserter = tracks_database_inserter
        self._audio_features_database_inserter = audio_features_database_inserter
        self._radio_tracks_database_inserter = radio_tracks_database_inserter

    async def collect(self, playlists_ids: List[str]) -> None:
        logger.info('Starting to run `RadioStationsSnapshotsCollector`')
        playlists = await self._spotify_client.playlists.collect(playlists_ids)
        await self._insert_records_to_db(playlists)
        logger.info('Successfully collected and inserted playlists to DB')

    async def _insert_records_to_db(self, playlists: List[dict]) -> None:
        for playlist in playlists:
            print(f'Starting to insert playlist `{playlist[ID]}` spotify records')
            tracks = playlist[TRACKS][ITEMS]
            spotify_records = await self._insert_spotify_records(tracks)
            await self._radio_tracks_database_inserter.insert(
                playlist=playlist,
                tracks=tracks,
                artists=spotify_records[ARTISTS]
            )

    async def _insert_spotify_records(self, tracks: List[dict]) -> Dict[str, List[BaseSpotifyORMModel]]:
        spotify_records = {}

        for inserter in self._ordered_database_inserters:
            records = await inserter.insert(tracks)
            spotify_records[inserter.name] = records

        return spotify_records

    @property
    def _ordered_database_inserters(self) -> List[BaseSpotifyDatabaseInserter]:
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
        artists_database_inserter=SpotifyArtistsDatabaseInserter(db_engine, spotify_client),
        albums_database_inserter=SpotifyAlbumsDatabaseInserter(db_engine),
        tracks_database_inserter=SpotifyTracksDatabaseInserter(db_engine),
        audio_features_database_inserter=SpotifyAudioFeaturesDatabaseInserter(db_engine, spotify_client),
        radio_tracks_database_inserter=RadioTracksDatabaseInserter(db_engine)
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(snapshots_collector.collect(['18cUFeM5Q75ViwevsMQM1j']))
