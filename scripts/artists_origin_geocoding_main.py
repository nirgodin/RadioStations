import asyncio

from data_collectors.components import ComponentFactory


async def run_google_artists_origin_geocoding_manager() -> None:
    component_factory = ComponentFactory()
    api_key = component_factory.env.get_google_geocoding_api_key()
    client_session = component_factory.sessions.get_google_geocoding_session(api_key)

    async with client_session as session:
        manager = component_factory.google.get_artists_origin_geocoding_manager(session)
        await manager.run(limit=15)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_google_artists_origin_geocoding_manager())
