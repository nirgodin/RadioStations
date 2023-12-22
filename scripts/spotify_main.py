import asyncio

from aiohttp import ClientSession
from data_collectors.components import ComponentFactory
from genie_datastores.postgres.models import SpotifyStation

from data_collection.spotify.collectors.radio_stations_snapshots.radio_stations_snapshots_collector import \
    RadioStationsSnapshotsCollector
from tools.environment_manager import EnvironmentManager
from utils.spotify_utils import build_spotify_headers

STATIONS = [
    SpotifyStation.GLGLZ,
    SpotifyStation.ECO_99,
    SpotifyStation.FM_103,
    SpotifyStation.KAN_GIMEL,
    SpotifyStation.KAN_88
]


async def run() -> None:
    EnvironmentManager().set_env_variables()

    async with ClientSession(headers=build_spotify_headers()) as session:
        radio_stations_snapshots_collector = RadioStationsSnapshotsCollector(session)
        await radio_stations_snapshots_collector.collect()

    #########################################################################

    component_factory = ComponentFactory()
    spotify_session = component_factory.sessions.get_spotify_session()

    async with spotify_session as session:
        snapshots_manager = component_factory.misc.get_radio_snapshots_manager(session)
        await snapshots_manager.run(STATIONS)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
