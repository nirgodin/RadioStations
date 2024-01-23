import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_glglz_charts_manager() -> None:
    factory = ComponentFactory()
    session = factory.sessions.get_spotify_session()

    async with session as spotify_session:
        manager = factory.radio_charts.get_glglz_charts_manager(spotify_session)
        await manager.run(dates=None, limit=6)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_glglz_charts_manager())
