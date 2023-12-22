import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run() -> None:
    factory = ComponentFactory()
    link_manager = factory.wikipedia.get_artists_age_link_manager()
    await link_manager.run(100)

    name_manager = factory.wikipedia.get_artists_age_name_manager()
    await name_manager.run(100)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
