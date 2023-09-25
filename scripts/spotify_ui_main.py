import asyncio

from data_collection.spotify.collectors.artists_ui_collector import SpotifyArtistUICollector


async def run(chunk_size: int, max_chunks_number: int) -> None:
    collector = SpotifyArtistUICollector(chunk_size=chunk_size, max_chunks_number=max_chunks_number)
    await collector.collect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(chunk_size=10, max_chunks_number=20))
