import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_genius_missing_ids_manager() -> None:
    component_factory = ComponentFactory()
    bearer_token = component_factory.env.get_genius_access_token()
    client_session = component_factory.sessions.get_genius_session(bearer_token)

    async with client_session as session:
        missing_ids_manager = component_factory.genius.get_missing_ids_manager(session)
        await missing_ids_manager.run(200)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_genius_missing_ids_manager())
