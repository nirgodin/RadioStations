import asyncio
from datetime import datetime
from typing import List

from data_collection_v2.billboard.billboard_charts_collector import BillboardChartsCollector
from data_collection_v2.billboard.billboard_tracks_collector import BillboardTracksCollector
from data_collection_v2.data_collection_component_factory import get_billboard_charts_collector, \
    get_billboard_tracks_collector, get_session
from data_collection_v2.spotify.spotify_insertions_manager import SpotifyInsertionsManager
from tools.environment_manager import EnvironmentManager


class BillboardManager:
    def __init__(self,
                 charts_collector: BillboardChartsCollector,
                 tracks_collector: BillboardTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager):
        self._charts_collector = charts_collector
        self._tracks_collector = tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager

    async def collect(self, dates: List[datetime]):
        charts = await self._charts_collector.collect(dates)
        entries = await self._tracks_collector.collect(charts)
        tracks = [entry.track for entry in entries]
        insertion_results = await self._spotify_insertions_manager.insert(tracks)
        print('b')


async def main(dates: List[datetime]):
    EnvironmentManager().set_env_variables()
    session = get_session()
    manager = BillboardManager(
        charts_collector=get_billboard_charts_collector(session),
        tracks_collector=get_billboard_tracks_collector(session)
    )
    await manager.collect(dates)


if __name__ == '__main__':
    DATES = [
        datetime(1958, 8, 4),
        datetime(1983, 4, 15),
        datetime(2017, 12, 19)
    ]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(DATES))
