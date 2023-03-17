import asyncio
from dataclasses import fields
from datetime import datetime

from aiohttp import ClientSession

from consts.path_consts import SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS
from data_collection.spotify.albums_details_collector import AlbumsDetailsCollector
from data_collection.spotify.artists_ids_collector import ArtistsIDsCollector
from data_collection.spotify.audio_features_collector import AudioFeaturesCollector
from data_collection.spotify.weekly_run.spotify_script_config import SpotifyScriptConfig
from data_collection.spotify.weekly_run.spotify_weekly_runner_config import SpotifyWeeklyRunnerConfig
from utils.datetime_utils import now_israel_timezone, DATETIME_FORMAT
from utils.file_utils import read_json
from utils.spotify_utils import build_spotify_headers


class SpotifyWeeklyRunner:
    def __init__(self, config: SpotifyWeeklyRunnerConfig):
        self._config = config
        self._weekly_run_latest_executions = read_json(SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS)

    def run(self) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._run_async())

    async def _run_async(self) -> None:
        headers = build_spotify_headers()

        async with ClientSession(headers=headers) as session:
            for script in fields(self._config):
                script_config: SpotifyScriptConfig = getattr(self._config, script.name)

                if self._should_run(script_config.name, script_config.weekday):
                    await self._run_script(session, script_config)

    def _should_run(self, script_name: str, script_weekday: int) -> bool:
        now = now_israel_timezone()

        if now.weekday() == script_weekday:
            if not self._has_script_been_executed_today(script_name, now):
                return True

        return False

    def _has_script_been_executed_today(self, script_name: str, now: datetime) -> bool:
        latest_script_execution = datetime.strptime(
            self._weekly_run_latest_executions[script_name],
            DATETIME_FORMAT
        )
        return now.date() == latest_script_execution.date()

    @staticmethod
    async def _run_script(session: ClientSession, script_config: SpotifyScriptConfig) -> None:
        print(f'Starting to run `{script_config.name}`')
        collector = script_config.clazz(
            session=session,
            chunk_size=script_config.chunk_size
        )
        await collector.collect()


if __name__ == '__main__':
    config = SpotifyWeeklyRunnerConfig(
        audio_features=SpotifyScriptConfig(
            name='audio features collector',
            weekday=4,
            clazz=AudioFeaturesCollector,
            chunk_size=1000
        ),
        artists_ids=SpotifyScriptConfig(
            name='artists ids collector',
            weekday=2,
            clazz=ArtistsIDsCollector,
            chunk_size=100
        ),
        albums_details=SpotifyScriptConfig(
            name='albums details collector',
            weekday=3,
            clazz=AlbumsDetailsCollector,
            chunk_size=50
        )
    )
    SpotifyWeeklyRunner(config).run()
