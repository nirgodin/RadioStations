import asyncio

from data_collectors.components import ComponentFactory

from tools.environment_manager import EnvironmentManager


async def run_genius_artist_ids_manager() -> None:
    factory = ComponentFactory()
    token = factory.env.get_genius_access_token()
    genius_session = factory.sessions.get_genius_session(token)

    async with genius_session as session:
        manager = factory.genius.get_artists_ids_manager(session)
        await manager.run(200)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_genius_artist_ids_manager())
