import asyncio

from aiohttp import ClientSession

from data_collection.genius.genius_lyrics_collector import GeniusLyricsCollector
from data_collection.genius.genius_search_collector import GeniusSearchCollector
from data_collection.genius.genius_songs_collector import GeniusSongsCollector
from tools.environment_manager import EnvironmentManager
from utils.genius_utils import build_genius_headers


async def run(chunk_size: int, max_chunks_number: int) -> None:
    EnvironmentManager().set_env_variables()
    headers = build_genius_headers()

    async with ClientSession(headers=headers) as session:
        for genius_collector in [GeniusSearchCollector, GeniusSongsCollector]:
            print(f'Starting to run {genius_collector.__name__}')
            collector = genius_collector(
                chunk_size=chunk_size,
                max_chunks_number=max_chunks_number,
                session=session
            )
            await collector.collect()

    async with GeniusLyricsCollector(chunk_size, max_chunks_number) as lyrics_collector:
        print(f'Starting to run GeniusLyricsCollector')
        await lyrics_collector.collect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(chunk_size=50, max_chunks_number=5))
