from dataclasses import dataclass

from data_collection.spotify.weekly_run.spotify_script_config import SpotifyScriptConfig


@dataclass
class SpotifyWeeklyRunnerConfig:
    audio_features: SpotifyScriptConfig
    artists_ids: SpotifyScriptConfig
    albums_details: SpotifyScriptConfig
