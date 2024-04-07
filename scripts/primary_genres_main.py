import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_primary_genres_manager() -> None:
    factory = ComponentFactory()
    manager = await factory.genres.get_primary_genre_manager()

    await manager.run(limit=500)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_primary_genres_manager())
