import asyncio

from data_collection.openai.embeddings.track_names_embeddings_collector import TrackNamesEmbeddingsCollector
from tools.environment_manager import EnvironmentManager


async def run(chunk_size: int, max_chunks_number: int) -> None:
    EnvironmentManager().set_env_variables()

    async with TrackNamesEmbeddingsCollector(chunk_size=chunk_size, chunks_limit=max_chunks_number) as collector:
        await collector.collect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(chunk_size=50, max_chunks_number=5))
