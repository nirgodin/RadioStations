import asyncio
from datetime import datetime
from typing import List

from postgres_client import get_database_engine

from data_collection_v2.billboard.billboard_charts_collector import BillboardChartsCollector
from data_collection_v2.billboard.billboard_tracks_collector import BillboardTracksCollector
from data_collection_v2.data_collection_component_factory import get_billboard_charts_collector, \
    get_billboard_tracks_collector, get_session, get_spotify_insertions_manager, get_spotify_client, \
    get_billboard_charts_inserter, get_billboard_tracks_inserter
from data_collection_v2.database_insertion.billboard_database_inserters.billboard_charts_database_inserter import \
    BillboardChartsDatabaseInserter
from data_collection_v2.database_insertion.billboard_database_inserters.billboard_tracks_database_inserter import \
    BillboardTracksDatabaseInserter
from data_collection_v2.database_insertion.billboard_database_inserters.billboard_tracks_updater import \
    BillboardTracksUpdater
from data_collection_v2.spotify.spotify_insertions_manager import SpotifyInsertionsManager
from tools.environment_manager import EnvironmentManager


class BillboardManager:
    def __init__(self,
                 charts_collector: BillboardChartsCollector,
                 tracks_collector: BillboardTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 tracks_inserter: BillboardTracksDatabaseInserter,
                 charts_inserter: BillboardChartsDatabaseInserter,
                 tracks_updater: BillboardTracksUpdater):
        self._charts_collector = charts_collector
        self._tracks_collector = tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._tracks_inserter = tracks_inserter
        self._charts_inserter = charts_inserter
        self._tracks_updater = tracks_updater

    async def collect(self, dates: List[datetime]):
        charts = await self._charts_collector.collect(dates)
        entries = await self._tracks_collector.collect(charts)
        tracks = [entry.track for entry in entries if isinstance(entry.track, dict)]
        await self._spotify_insertions_manager.insert(tracks)
        await self._tracks_inserter.insert(entries)
        await self._charts_inserter.insert(entries)
        await self._tracks_updater.update(entries)


async def main(dates: List[datetime]):
    EnvironmentManager().set_env_variables()
    session = get_session()
    spotify_client = get_spotify_client(session)
    db_engine = get_database_engine()
    manager = BillboardManager(
        charts_collector=get_billboard_charts_collector(session),
        tracks_collector=get_billboard_tracks_collector(session, spotify_client),
        spotify_insertions_manager=get_spotify_insertions_manager(spotify_client, db_engine),
        charts_inserter=get_billboard_charts_inserter(db_engine),
        tracks_inserter=get_billboard_tracks_inserter(db_engine)
    )
    await manager.collect(dates)
