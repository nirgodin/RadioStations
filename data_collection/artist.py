from collections import Counter
from dataclasses import dataclass
from typing import List, Dict, Any

from dacite import from_dict

from consts.data_consts import ARTIST_NAME, MAIN_GENRE, GENRES, ARTIST_POPULARITY, ARTIST_FOLLOWERS
from data_collection.spotify import get_spotipy

MAIN_GENRES = [
    'rock',
    'pop',
    'hip',
    'soul',
    'blues',
    'funk',
    'jazz',
    'reggae',
    'folk',
    'singer-songwriter'
]

GENRES_DICT = {
    'punk': 'rock',
    'rock-and-roll': 'rock',
    'k-pop': 'pop',
    'rap': 'hip',
    'r&b': 'soul',
    'mexican': 'world',
    'cha-cha-cha': 'world',
    'bossa': 'world',
    'latin': 'world',
    'country': 'folk'
}

TOTAL = 'total'


@dataclass
class Artist:
    name: str
    popularity: int
    followers: Dict[str, Any]
    genres: List[str]
    _sp = get_spotipy()

    @classmethod
    def create_from_id(cls, id: str):
        spotify = get_spotipy()
        artist_page = spotify.artist(artist_id=id)
        return from_dict(data_class=Artist, data=artist_page)

    def to_dict(self):
        return {
            ARTIST_NAME: self.name,
            ARTIST_FOLLOWERS: self.artist_followers,
            ARTIST_POPULARITY: self.popularity,
            GENRES: self.genres,
            MAIN_GENRE: self.main_genre
        }

    @property
    def artist_followers(self) -> int:
        return self.followers.get(TOTAL, -1)

    @property
    def main_genre(self) -> str:
        for genre in self._yield_most_common_genre():
            if genre in MAIN_GENRES:
                return genre
            elif genre in GENRES_DICT.keys():
                return GENRES_DICT[genre]

        return 'other'

    def _yield_most_common_genre(self):
        if not self.genres:
            return ''

        genre_concat = ' '.join(self.genres)
        genre_tokens = genre_concat.split(' ') # Concatenating and splitting is done in order to break two-worded genres

        return (genre[0] for genre in Counter(genre_tokens).most_common())
