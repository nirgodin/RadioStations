import os
from functools import lru_cache
from typing import Dict, Optional

import spotipy
from spotipy import SpotifyClientCredentials

from consts.env_consts import SPOTIPY_PORT
from data_collection.spotify.access_token_generator import AccessTokenGenerator


@lru_cache(maxsize=1)
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


@lru_cache(maxsize=1)
def get_port() -> int:
    return int(os.environ[SPOTIPY_PORT])


@lru_cache(maxsize=1)
def build_spotify_authorization_headers() -> Dict[str, str]:
    bearer_token = AccessTokenGenerator.generate()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Basic {bearer_token}"
    }
