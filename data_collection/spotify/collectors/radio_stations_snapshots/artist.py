from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Artist:
    artist_name: str
    artist_popularity: int
    artist_followers: int
    genres: List[str]
    main_genre: str
