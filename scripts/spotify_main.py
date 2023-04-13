import asyncio

from data_collection.spotify.collectors.albums_details_collector import AlbumsDetailsCollector
from data_collection.spotify.collectors.artists_ids_collector import ArtistsIDsCollector
from data_collection.spotify.collectors.audio_features_collector import AudioFeaturesCollector
from data_collection.spotify.weekly_run.spotify_collector_config import SpotifyCollectorConfig
from data_collection.spotify.weekly_run.spotify_weekly_runner import SpotifyWeeklyRunner
from data_collection.spotify.radio_stations_snapshots_runner import RadioStationsSnapshotsRunner
from data_collection.spotify.weekly_run.spotify_weekly_runner_config import SpotifyWeeklyRunnerConfig
from tools.environment_manager import EnvironmentManager
from utils.general_utils import is_remote_run


async def run() -> None:
    EnvironmentManager().set_env_variables()
    radio_stations_snapshots_runner = RadioStationsSnapshotsRunner()
    radio_stations_snapshots_runner.run()

    if not is_remote_run():
        spotify_weekly_run_config = SpotifyWeeklyRunnerConfig(
            artists_ids=SpotifyCollectorConfig(
                name='artists ids collector',
                weekday=2,
                collector=ArtistsIDsCollector,
                chunk_size=100,
                max_chunks_number=10
            ),
            albums_details=SpotifyCollectorConfig(
                name='albums details collector',
                weekday=3,
                collector=AlbumsDetailsCollector,
                chunk_size=50,
                max_chunks_number=10
            ),
            audio_features=SpotifyCollectorConfig(
                name='audio features collector',
                weekday=4,
                collector=AudioFeaturesCollector,
                chunk_size=1000,
                max_chunks_number=5
            )
        )
        weekly_runner = SpotifyWeeklyRunner(spotify_weekly_run_config)
        await weekly_runner.run()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
