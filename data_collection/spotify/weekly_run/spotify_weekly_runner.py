import asyncio
from dataclasses import fields
from datetime import datetime

from aiohttp import ClientSession

from consts.path_consts import SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS
from data_collection.spotify.collectors.albums_details_collector import AlbumsDetailsCollector
from data_collection.spotify.collectors.artists_ids_collector import ArtistsIDsCollector
from data_collection.spotify.collectors.audio_features_collector import AudioFeaturesCollector
from data_collection.spotify.weekly_run.spotify_collector_config import SpotifyCollectorConfig
from data_collection.spotify.weekly_run.spotify_weekly_runner_config import SpotifyWeeklyRunnerConfig
from utils.datetime_utils import now_israel_timezone, DATETIME_FORMAT
from utils.file_utils import read_json, to_json
from utils.spotify_utils import build_spotify_headers


class SpotifyWeeklyRunner:
    def __init__(self, config: SpotifyWeeklyRunnerConfig):
        self._config = config
        self._weekly_run_latest_executions = read_json(SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS)

    async def run(self) -> None:
        for script in fields(self._config):
            script_config: SpotifyCollectorConfig = getattr(self._config, script.name)

            if self._should_run(script_config.name, script_config.weekday):
                async with ClientSession(headers=build_spotify_headers()) as session:
                    await self._run_script(session, script_config)

                self._update_script_execution_date(script_config.name)

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
    async def _run_script(session: ClientSession, script_config: SpotifyCollectorConfig) -> None:
        print(f'Starting to run `{script_config.name}`')
        collector = script_config.collector(
            session=session,
            chunk_size=script_config.chunk_size
        )
        await collector.collect()

    def _update_script_execution_date(self, script_name: str) -> None:
        self._weekly_run_latest_executions[script_name] = now_israel_timezone().strftime(DATETIME_FORMAT)
        to_json(d=self._weekly_run_latest_executions, path=SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS)
