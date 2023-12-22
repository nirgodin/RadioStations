import asyncio

from aiohttp import ClientSession
from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_spotify_images_gender_manager():
    component_factory = ComponentFactory()
    raw_spotify_session = component_factory.sessions.get_spotify_session()

    async with ClientSession() as session:
        async with raw_spotify_session as spotify_session:
            gender_manager = component_factory.spotify.get_artists_images_gender_manager(
                session,
                spotify_session,
                0.85
            )
            await gender_manager.run(limit=100)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_spotify_images_gender_manager())
