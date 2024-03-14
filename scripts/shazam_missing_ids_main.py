import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_shazam_missing_ids_manager() -> None:
    component_factory = ComponentFactory()
    missing_ids_manager = component_factory.shazam.get_missing_ids_manager()
    await missing_ids_manager.run(200)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_shazam_missing_ids_manager())
