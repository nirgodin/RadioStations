import asyncio

from aiohttp import ClientSession

from data_collection.spotify.collectors.tracks_albums_details_collector import TracksAlbumsDetailsCollector
from tools.environment_manager import EnvironmentManager
from utils.spotify_utils import build_spotify_headers


async def main():
    EnvironmentManager().set_env_variables()

    async with ClientSession(headers=build_spotify_headers()) as session:
        collector = TracksAlbumsDetailsCollector(session, 100, 10)
        await collector.collect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
