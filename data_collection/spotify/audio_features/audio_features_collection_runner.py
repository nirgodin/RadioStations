import asyncio

from aiohttp import ClientSession

from data_collection.spotify.audio_features.audio_features_collector import AudioFeaturesCollector
from utils.datetime_utils import now_israel_timezone
from utils.spotify_utils import build_spotify_headers


class AudioFeaturesCollectorRunner:
    def run(self, chunk_size: int = 1000) -> None:
        if self._should_run():
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._run_async(chunk_size))

    @staticmethod
    def _should_run() -> bool:
        now = now_israel_timezone()

        if now.weekday() == 4:
            if 5 <= now.hour <= 13:
                return True

        return False

    @staticmethod
    async def _run_async(chunk_size: int) -> None:
        headers = build_spotify_headers()

        async with ClientSession(headers=headers) as session:
            collector = AudioFeaturesCollector(session=session, chunk_size=chunk_size)
            await collector.collect()


if __name__ == '__main__':
    AudioFeaturesCollectorRunner().run()