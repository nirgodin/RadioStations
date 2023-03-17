from functools import lru_cache
from typing import Dict

import spotipy
from spotipy import SpotifyClientCredentials

from data_collection.spotify.access_token_generator import AccessTokenGenerator


def build_spotify_headers() -> Dict[str, str]:
    bearer_token = AccessTokenGenerator.generate()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }


@lru_cache(maxsize=1)
def get_spotipy():
    return spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
