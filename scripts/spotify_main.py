import asyncio

from aiohttp import ClientSession

from consts.data_consts import IS_ISRAELI
from consts.miscellaneous_consts import NAMED_PLAYLISTS, RECORD_KEY, RECORD_VALUE, OUTPUT_PATH
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import SPOTIFY_EQUAL_PLAYLISTS_OUTPUT_PATH, SPOTIFY_ISRAELI_PLAYLISTS_OUTPUT_PATH
from consts.playlists_consts import EQUAL_PLAYLISTS, ISRAELI_PLAYLISTS
from data_collection.spotify.collectors.albums_details_collector import AlbumsDetailsCollector
from data_collection.spotify.collectors.artists_ids_collector import ArtistsIDsCollector
from data_collection.spotify.collectors.audio_features_collector import AudioFeaturesCollector
from data_collection.spotify.collectors.playlists_artists_collector import PlaylistsArtistsCollector
from data_collection.spotify.collectors.radio_stations_snapshots.radio_stations_snapshots_collector import \
    RadioStationsSnapshotsCollector
from data_collection.spotify.collectors.tracks_ids_collector import TracksIDsCollector
from data_collection.spotify.weekly_run.spotify_collector_config import SpotifyCollectorConfig
from data_collection.spotify.weekly_run.spotify_weekly_runner import SpotifyWeeklyRunner
from data_collection.wikipedia.gender.genders import Genders
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
                name='tracks ids collector',
                weekday=5,
                collector=TracksIDsCollector,
                chunk_size=300,
                max_chunks_number=5
            ),
            SpotifyCollectorConfig(
                name='equal playlists collector',
                weekday=6,
                collector=PlaylistsArtistsCollector,
                chunk_size=len(EQUAL_PLAYLISTS),
                max_chunks_number=2,
                kwargs={
                    NAMED_PLAYLISTS: EQUAL_PLAYLISTS,
                    RECORD_KEY: ARTIST_GENDER,
                    RECORD_VALUE: Genders.FEMALE.value,
                    OUTPUT_PATH: SPOTIFY_EQUAL_PLAYLISTS_OUTPUT_PATH
                }
            ),
            SpotifyCollectorConfig(
                name='israeli playlists collector',
                weekday=7,
                collector=PlaylistsArtistsCollector,
                chunk_size=len(ISRAELI_PLAYLISTS),
                max_chunks_number=2,
                kwargs={
                    NAMED_PLAYLISTS: ISRAELI_PLAYLISTS,
                    RECORD_KEY: IS_ISRAELI,
                    RECORD_VALUE: True,
                    OUTPUT_PATH: SPOTIFY_ISRAELI_PLAYLISTS_OUTPUT_PATH
                }
            )
        ]
        weekly_runner = SpotifyWeeklyRunner(spotify_weekly_run_config)
        await weekly_runner.run()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
