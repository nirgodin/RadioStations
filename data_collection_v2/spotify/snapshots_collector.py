import asyncio
from typing import List, Dict

from aiohttp import ClientSession
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from genie_datastores.postgres.operations import get_database_engine
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
from data_collection_v2.database_insertion.spotify_database_inserters.track_id_mapping_inserter import \
    TrackIDMappingDatabaseInserter
from data_collection_v2.spotify.spotify_insertions_manager import SpotifyInsertionsManager
from tools.environment_manager import EnvironmentManager
from tools.logging import logger
from utils.spotify_utils import build_spotify_headers


class RadioStationsSnapshotsCollector:
    def __init__(self,
                 spotify_client: SpotifyClient,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 radio_tracks_database_inserter: RadioTracksDatabaseInserter):
        self._spotify_client = spotify_client
        self._spotify_insertions_manager = spotify_insertions_manager
        self._radio_tracks_database_inserter = radio_tracks_database_inserter

    async def collect(self, playlists_ids: List[str]) -> None:
        logger.info('Starting to run `RadioStationsSnapshotsCollector`')
        playlists = await self._spotify_client.playlists.collect(playlists_ids)
        await self._insert_records_to_db(playlists)
        logger.info('Successfully collected and inserted playlists to DB')

    async def _insert_records_to_db(self, playlists: List[dict]) -> None:
        for playlist in playlists:
            logger.info(f'Starting to insert playlist `{playlist[ID]}` spotify records')
            tracks = playlist[TRACKS][ITEMS]
            spotify_records = await self._spotify_insertions_manager.insert(tracks)
            await self._insert_radio_tracks(
                playlist=playlist,
                tracks=tracks,
                artists=spotify_records[ARTISTS]
            )

    async def _insert_radio_tracks(self, playlist: dict, tracks: List[dict], artists: List[SpotifyArtist]) -> None:
        artists_ids = [artist.id for artist in artists]
        artists_responses = await self._spotify_client.artists.info.collect(artists_ids)

        await self._radio_tracks_database_inserter.insert(
            playlist=playlist,
            tracks=tracks,
            artists=artists_responses
        )


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    session = ClientSession(headers=build_spotify_headers())
    db_engine = get_database_engine()
    spotify_client = SpotifyClient.create(session)
    snapshots_collector = RadioStationsSnapshotsCollector(
        spotify_client=spotify_client,
        spotify_insertions_manager=SpotifyInsertionsManager(
            artists_database_inserter=SpotifyArtistsDatabaseInserter(db_engine, spotify_client),
            albums_database_inserter=SpotifyAlbumsDatabaseInserter(db_engine),
            tracks_database_inserter=SpotifyTracksDatabaseInserter(db_engine),
            audio_features_database_inserter=SpotifyAudioFeaturesDatabaseInserter(db_engine, spotify_client),
            track_id_mapping_database_inserter=TrackIDMappingDatabaseInserter(db_engine)
        ),
        radio_tracks_database_inserter=RadioTracksDatabaseInserter(db_engine),
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(snapshots_collector.collect(['18cUFeM5Q75ViwevsMQM1j']))
