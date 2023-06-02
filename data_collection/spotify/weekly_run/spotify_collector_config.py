from dataclasses import dataclass, field
from typing import Type

from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector


@dataclass
class SpotifyCollectorConfig:
    name: str
    weekday: int
    chunk_size: int
    max_chunks_number: int
    collector: Type[BaseSpotifyCollector]
    kwargs: dict = field(default_factory=dict)
