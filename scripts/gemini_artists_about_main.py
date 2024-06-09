import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run():
    EnvironmentManager().set_env_variables()
    factory = ComponentFactory()
    manager = factory.google.get_spotify_artists_about_manager()

    await manager.run(limit=100)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
