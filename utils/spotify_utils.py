from typing import Dict

from data_collection.spotify.access_token_generator import AccessTokenGenerator


def build_spotify_headers() -> Dict[str, str]:
    bearer_token = AccessTokenGenerator.generate()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }


def is_access_token_expired(response: dict) -> bool:
    return response.get('error', {}).get('status') == 401
