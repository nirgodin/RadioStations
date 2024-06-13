import asyncio

from data_collectors.components import ComponentFactory
from genie_datastores.postgres.models import SpotifyStation

from tools.environment_manager import EnvironmentManager

STATIONS = [
    SpotifyStation.GLGLZ,
    SpotifyStation.ECO_99,
    SpotifyStation.FM_103,
    SpotifyStation.KAN_GIMEL,
    SpotifyStation.KAN_88,
    SpotifyStation.KZ_RADIO
]


async def run() -> None:
    EnvironmentManager().set_env_variables()

    component_factory = ComponentFactory()
    spotify_session = component_factory.sessions.get_spotify_session()

    async with spotify_session as session:
        snapshots_manager = component_factory.misc.get_radio_snapshots_manager(session)
        await snapshots_manager.run(STATIONS)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
