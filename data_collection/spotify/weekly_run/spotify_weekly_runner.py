from datetime import datetime
from typing import List

from aiohttp import ClientSession

from consts.path_consts import SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS
from data_collection.spotify.weekly_run.spotify_collector_config import SpotifyCollectorConfig
from utils.datetime_utils import now_israel_timezone, DATETIME_FORMAT
from utils.file_utils import read_json, to_json
from utils.spotify_utils import build_spotify_headers
from utils.vcs_utils import commit_and_push


class SpotifyWeeklyRunner:
    def __init__(self, config: List[SpotifyCollectorConfig]):
        self._config = config
        self._weekly_run_latest_executions = read_json(SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS)

    async def run(self) -> None:
        for script_config in self._config:
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
            chunk_size=script_config.chunk_size,
            max_chunks_number=script_config.max_chunks_number
        )
        await collector.collect(**script_config.kwargs)

    def _update_script_execution_date(self, script_name: str) -> None:
        self._weekly_run_latest_executions[script_name] = now_israel_timezone().strftime(DATETIME_FORMAT)
        to_json(d=self._weekly_run_latest_executions, path=SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS)
        commit_and_push(
            files_paths=[SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS],
            commit_message=f'Ran `{script_name}` script'
        )
