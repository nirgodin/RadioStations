import asyncio

from data_collectors.components import ComponentFactory

from data_collection.mako_hit_list.mako_hit_list_crawler import MakoHitListCrawler
from tools.environment_manager import EnvironmentManager


async def run_mako_hit_list_charts_manager() -> None:
    MakoHitListCrawler().crawl()
    factory = ComponentFactory()
    session = factory.sessions.get_spotify_session()

    async with session as spotify_session:
        manager = factory.radio_charts.get_mako_hit_list_charts_manager(spotify_session)
        await manager.run()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_mako_hit_list_charts_manager())
