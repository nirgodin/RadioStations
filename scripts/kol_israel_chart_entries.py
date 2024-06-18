import asyncio

from data_collectors.components import ComponentFactory
from genie_datastores.postgres.models import Chart

from tools.environment_manager import EnvironmentManager


async def run_radio_charts_manager() -> None:
    factory = ComponentFactory()
    spotify_session = factory.sessions.get_spotify_session()

    async with spotify_session as session:
        manager = factory.charts.get_translated_artist_radio_charts_manager(session)
        await manager.run(chart=Chart.KOL_ISRAEL_WEEKLY_INTERNATIONAL, limit=1)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_radio_charts_manager())
