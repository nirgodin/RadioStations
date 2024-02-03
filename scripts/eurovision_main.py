import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_eurovision_manager() -> None:
    factory = ComponentFactory()
    raw_client_session = factory.sessions.get_client_session()
    raw_spotify_session = factory.sessions.get_spotify_session()

    async with raw_client_session as client_session:
        async with raw_spotify_session as spotify_session:
            manager = factory.charts.get_eurovision_charts_manager(client_session, spotify_session)
            await manager.run(years=None, limit=5)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_eurovision_manager())
