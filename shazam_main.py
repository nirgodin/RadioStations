import asyncio

import pandas as pd

from consts.path_consts import MERGED_DATA_PATH
from data_collection.shazam.shazam_city_fetcher import ShazamCitiesFetcher
from data_collection.shazam.shazam_country_fetcher import ShazamCountryFetcher
from data_collection.shazam.shazam_search_fetcher import ShazamSearchFetcher


async def run():
    cities_fetcher = ShazamCitiesFetcher()
    print('Starting to fetch data of israeli cities')
    await cities_fetcher.fetch_cities_top_tracks()

    country_fetcher = ShazamCountryFetcher()
    print('Starting to fetch data of Israel')
    await country_fetcher.fetch_country_top_tracks()

    print('Starting to fetch data of world')
    await country_fetcher.fetch_world_top_tracks()

    search_fetcher = ShazamSearchFetcher()
    print('Starting to fetch tracks ids')
    data = pd.read_csv(MERGED_DATA_PATH)
    await search_fetcher.search_tracks(data=data, max_tracks=100)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
