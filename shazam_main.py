import asyncio

from data_collection.shazam.shazam_city_fetcher import ShazamCitiesFetcher

if __name__ == '__main__':
    cities_fetcher = ShazamCitiesFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cities_fetcher.fetch_cities_top_tracks())
