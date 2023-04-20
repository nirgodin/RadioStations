import asyncio

from aiohttp import ClientSession

from consts.playlists_consts import EQUAL_PLAYLISTS
from data_collection.spotify.collectors.albums_details_collector import AlbumsDetailsCollector
from data_collection.spotify.collectors.artists_ids_collector import ArtistsIDsCollector
from data_collection.spotify.collectors.audio_features_collector import AudioFeaturesCollector
from data_collection.spotify.collectors.equal_playlists_collector import EqualPlaylistsCollector
from data_collection.spotify.collectors.radio_stations_snapshots.radio_stations_snapshots_collector import \
    RadioStationsSnapshotsCollector
from data_collection.spotify.weekly_run.spotify_collector_config import SpotifyCollectorConfig
from data_collection.spotify.weekly_run.spotify_weekly_runner import SpotifyWeeklyRunner
from tools.environment_manager import EnvironmentManager
from utils.general_utils import is_remote_run
from utils.spotify_utils import build_spotify_headers


async def run() -> None:
    EnvironmentManager().set_env_variables()

    async with ClientSession(headers=build_spotify_headers()) as session:
        radio_stations_snapshots_collector = RadioStationsSnapshotsCollector(session)
        await radio_stations_snapshots_collector.collect()

    if not is_remote_run():
        spotify_weekly_run_config = [
            SpotifyCollectorConfig(
                name='artists ids collector',
                weekday=2,
                collector=ArtistsIDsCollector,
                chunk_size=100,
                max_chunks_number=10
            ),
            SpotifyCollectorConfig(
                name='albums details collector',
                weekday=3,
                collector=AlbumsDetailsCollector,
                chunk_size=50,
                max_chunks_number=10
            ),
            SpotifyCollectorConfig(
                name='audio features collector',
                weekday=4,
                collector=AudioFeaturesCollector,
                chunk_size=1000,
                max_chunks_number=5
            ),
            SpotifyCollectorConfig(
                name='equal playlists collector',
                weekday=6,
                collector=EqualPlaylistsCollector,
                chunk_size=len(EQUAL_PLAYLISTS),
                max_chunks_number=2
            )
        ]
        weekly_runner = SpotifyWeeklyRunner(spotify_weekly_run_config)
        await weekly_runner.run()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
