import asyncio
import os

from data_collectors.components import ComponentFactory
from genie_datastores.milvus import MilvusClient

from tools.environment_manager import EnvironmentManager


async def run(limit: int) -> None:
    EnvironmentManager().set_env_variables()
    factory = ComponentFactory()
    openai_api_key = factory.env.get_openai_api_key()
    milvus_uri = factory.env.get_milvus_uri()
    milvus_token = os.environ["MILVUS_TOKEN"]

    async with factory.sessions.get_openai_session(openai_api_key) as session:
        async with MilvusClient(uri=milvus_uri, token=milvus_token) as client:
            manager = factory.misc.get_track_names_embeddings_manager(
                client_session=session,
                milvus_client=client
            )
            await manager.run(limit=limit)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(limit=100))
