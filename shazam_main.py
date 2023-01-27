import asyncio

from data_collection.shazam.shazam_city_fetcher import ShazamCitiesFetcher
from data_collection.shazam.shazam_country_fetcher import ShazamCountryFetcher


async def run():
    cities_fetcher = ShazamCitiesFetcher()
    print('Starting to fetch data of israeli cities')
    await cities_fetcher.fetch_cities_top_tracks()

    country_fetcher = ShazamCountryFetcher()
    print('Starting to fetch data of Israel')
    await country_fetcher.fetch_country_top_tracks()

    print('Starting to fetch data of World')
    await country_fetcher.fetch_world_top_tracks()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
