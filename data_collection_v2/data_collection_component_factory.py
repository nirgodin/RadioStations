from functools import lru_cache
from ssl import create_default_context
from typing import Optional

from aiohttp import ClientSession, TCPConnector, CookieJar
from certifi import where
from postgres_client import get_database_engine
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collection_v2.billboard.billboard_charts_collector import BillboardChartsCollector
from data_collection_v2.billboard.billboard_tracks_collector import BillboardTracksCollector
from data_collection_v2.database_insertion.billboard_database_inserters.billboard_charts_database_inserter import \
    BillboardChartsDatabaseInserter
from data_collection_v2.database_insertion.billboard_database_inserters.billboard_tracks_database_inserter import \
    BillboardTracksDatabaseInserter
from data_collection_v2.database_insertion.radio_tracks_database_inserter import RadioTracksDatabaseInserter
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
from data_collection_v2.spotify.snapshots_collector import RadioStationsSnapshotsCollector
from data_collection_v2.spotify.spotify_insertions_manager import SpotifyInsertionsManager
from utils.spotify_utils import build_spotify_headers


def get_session() -> ClientSession:
    ssl_context = create_default_context(cafile=where())

    return ClientSession(
        headers=build_spotify_headers(),
        connector=TCPConnector(ssl=ssl_context),
        cookie_jar=CookieJar(quote_cookie=False)
    )


def get_spotify_client(session: Optional[ClientSession] = None) -> SpotifyClient:
    client_session = session or get_session()
    return SpotifyClient.create(client_session)


def get_spotify_insertions_manager(spotify_client: Optional[SpotifyClient] = None,
                                   db_engine: Optional[AsyncEngine] = None) -> SpotifyInsertionsManager:
    engine = db_engine or get_database_engine()
    client = spotify_client or get_spotify_client()

    return SpotifyInsertionsManager(
        artists_database_inserter=SpotifyArtistsDatabaseInserter(engine, client),
        albums_database_inserter=SpotifyAlbumsDatabaseInserter(engine),
        tracks_database_inserter=SpotifyTracksDatabaseInserter(engine),
        audio_features_database_inserter=SpotifyAudioFeaturesDatabaseInserter(engine, client),
        track_id_mapping_database_inserter=TrackIDMappingDatabaseInserter(engine)
    )


def get_snapshots_collector() -> RadioStationsSnapshotsCollector:
    spotify_client = get_spotify_client()
    db_engine = get_database_engine()

    return RadioStationsSnapshotsCollector(
        spotify_client=spotify_client,
        spotify_insertions_manager=get_spotify_insertions_manager(spotify_client, db_engine),
        radio_tracks_database_inserter=RadioTracksDatabaseInserter(db_engine)
    )


def get_billboard_charts_collector(session: Optional[ClientSession] = None) -> BillboardChartsCollector:
    client_session = session or get_session()
    return BillboardChartsCollector(client_session)


def get_billboard_tracks_collector(session: Optional[ClientSession] = None,
                                   spotify_client: Optional[SpotifyClient] = None) -> BillboardTracksCollector:
    client_session = session or get_session()
    client = spotify_client or get_spotify_client(client_session)

    return BillboardTracksCollector(
        session=client_session,
        spotify_client=client
    )


def get_billboard_tracks_inserter(db_engine: Optional[AsyncEngine] = None) -> BillboardTracksDatabaseInserter:
    engine = db_engine or get_database_engine()
    return BillboardTracksDatabaseInserter(engine)


def get_billboard_charts_inserter(db_engine: Optional[AsyncEngine] = None) -> BillboardChartsDatabaseInserter:
    engine = db_engine or get_database_engine()
    return BillboardChartsDatabaseInserter(engine)
