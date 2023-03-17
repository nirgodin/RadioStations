from dataclasses import dataclass

from data_collection.spotify.weekly_run.spotify_collector_config import SpotifyCollectorConfig


@dataclass
class SpotifyWeeklyRunnerConfig:
    audio_features: SpotifyCollectorConfig
    artists_ids: SpotifyCollectorConfig
    albums_details: SpotifyCollectorConfig
