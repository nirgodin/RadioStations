import asyncio

from data_collection.shazam.shazam_artists_ids_fetcher import ShazamArtistsIDsFetcher
from tools.environment_manager import EnvironmentManager


async def run():
    EnvironmentManager().set_env_variables()
    artists_ids_fetcher = ShazamArtistsIDsFetcher()
    await artists_ids_fetcher.fetch()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
