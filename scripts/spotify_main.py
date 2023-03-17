from data_collection.spotify.albums_details_collector import AlbumsDetailsCollector
from data_collection.spotify.artists_ids_collector import ArtistsIDsCollector
from data_collection.spotify.audio_features_collector import AudioFeaturesCollector
from data_collection.spotify.weekly_run.spotify_script_config import SpotifyScriptConfig
from data_collection.spotify.weekly_run.spotify_weekly_runner import SpotifyWeeklyRunner
from data_collection.spotify.radio_stations_snapshots_runner import RadioStationsSnapshotsRunner
from data_collection.spotify.weekly_run.spotify_weekly_runner_config import SpotifyWeeklyRunnerConfig


def run() -> None:
    RadioStationsSnapshotsRunner().run()
    spotify_weekly_run_config = SpotifyWeeklyRunnerConfig(
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
        ),
        audio_features=SpotifyScriptConfig(
            name='audio features collector',
            weekday=4,
            clazz=AudioFeaturesCollector,
            chunk_size=1000
        )
    )
    SpotifyWeeklyRunner(spotify_weekly_run_config).run()


if __name__ == '__main__':
    run()
