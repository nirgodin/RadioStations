from dataclasses import dataclass
from typing import Any


@dataclass
class SpotifyScriptConfig:
    name: str
    weekday: int
    chunk_size: int
    clazz: Any
