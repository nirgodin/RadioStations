from functools import lru_cache
from functools import lru_cache
from typing import Dict, Iterable

import spotipy
from spotipy import SpotifyClientCredentials

from data_collection.spotify.access_token_generator import AccessTokenGenerator
from data_collection.spotify.spotify_scope import SpotifyScope


@lru_cache(maxsize=1)
def build_spotify_headers(is_authorization_token: bool = False, scopes: Iterable[SpotifyScope] = ()) -> Dict[str, str]:
    bearer_token = AccessTokenGenerator.generate(is_authorization_token, scopes)
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }


@lru_cache(maxsize=1)
def get_spotipy():
    return spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


def build_spotify_query(artist: str, track: str) -> str:
    return f'{artist} {track}'
