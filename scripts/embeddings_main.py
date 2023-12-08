import asyncio

from data_collectors.components import ComponentFactory
from genie_datastores.milvus import MilvusClient

from tools.environment_manager import EnvironmentManager


async def run(limit: int) -> None:
    EnvironmentManager().set_env_variables()
    factory = ComponentFactory()
    milvus_uri = factory.env.get_milvus_uri()
    openai_api_key = factory.env.get_openai_api_key()

    async with factory.sessions.get_openai_session(openai_api_key) as session:
        async with MilvusClient(uri=milvus_uri) as client:
            manager = factory.get_track_names_embeddings_manager(
                client_session=session,
                milvus_client=client
            )
            await manager.run(limit=limit)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(limit=100))
