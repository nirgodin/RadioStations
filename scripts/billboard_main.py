import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_billboard_manager():
    component_factory = ComponentFactory()
    client_session = component_factory.sessions.get_client_session()
    raw_spotify_session = component_factory.sessions.get_spotify_session()

    async with raw_spotify_session as spotify_session:
        async with client_session as session:
            billboard_manager = component_factory.get_billboard_manager(spotify_session, session)
            await billboard_manager.run()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_billboard_manager())
