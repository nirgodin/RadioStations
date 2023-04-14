from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from consts.data_consts import NAME, POPULARITY, FOLLOWERS, TOTAL, GENRES


@dataclass_json
@dataclass
class Artist:
    artist_name: str
    artist_popularity: int
    artist_followers: int
    genres: List[str]
    main_genre: str

    @classmethod
    def from_spotify_response(cls, response: dict) -> "Artist":
        return cls(
            artist_name=response.get(NAME, ''),
            artist_popularity=response.get(POPULARITY, -1),
            artist_followers=response.get(FOLLOWERS, {}).get(TOTAL, -1),
            genres=response.get(GENRES, []),
            main_genre=''
        )
