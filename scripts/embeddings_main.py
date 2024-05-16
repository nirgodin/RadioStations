import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run(limit: int) -> None:
    EnvironmentManager().set_env_variables()
    factory = ComponentFactory()

    async with factory.tools.get_milvus_client() as milvus_client:
        retrieval_manager = factory.misc.get_track_names_embeddings_retriever(milvus_client)
        await retrieval_manager.run()

    collection_manager = factory.misc.get_track_names_embeddings_manager()
    await collection_manager.run(limit=limit)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(limit=100))
