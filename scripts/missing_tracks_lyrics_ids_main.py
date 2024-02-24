import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_missing_tracks_lyrics() -> None:
    factory = ComponentFactory()
    manager = factory.misc.get_lyrics_missing_ids_manager()
    await manager.run(None)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_missing_tracks_lyrics())
