import asyncio

from data_collectors.components import ComponentFactory

from data_collection.shazam.shazam_city_fetcher import ShazamCitiesFetcher
from data_collection.shazam.shazam_country_fetcher import ShazamCountryFetcher
from tools.environment_manager import EnvironmentManager


async def run_shazam_top_tracks_manager(component_factory: ComponentFactory) -> None:
    top_tracks_manager = component_factory.shazam.get_top_tracks_manager()
    await top_tracks_manager.run()


async def run_shazam_missing_ids_manager(component_factory: ComponentFactory) -> None:
    missing_ids_manager = component_factory.shazam.get_missing_ids_manager()
    await missing_ids_manager.run(100)


async def run():
    EnvironmentManager().set_env_variables()
    component_factory = ComponentFactory()
    await run_shazam_top_tracks_manager(component_factory)
    await run_shazam_missing_ids_manager(component_factory)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
