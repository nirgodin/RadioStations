import asyncio

from data_collection.wikipedia.age.wikipedia_link_age_collector import WikipediaAgeLinkCollector


async def run() -> None:
    collector = WikipediaAgeLinkCollector()
    await collector.collect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
