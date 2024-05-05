import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_shazam_origin_copy_manager() -> None:
    component_factory = ComponentFactory()
    manager = component_factory.shazam.get_origin_copy_manager()
    await manager.run(limit=None)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_shazam_origin_copy_manager())
