import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_spotify_charts_manager() -> None:
    factory = ComponentFactory()
    session = factory.sessions.get_spotify_session()

    async with session as spotify_session:
        manager = factory.charts.get_spotify_charts_manager(spotify_session)
        await manager.run()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_spotify_charts_manager())
