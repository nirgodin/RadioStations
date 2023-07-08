import os
from functools import lru_cache
from typing import Dict

from consts.env_consts import GENIUS_CLIENT_ACCESS_TOKEN


@lru_cache(maxsize=1)
def build_genius_headers() -> Dict[str, str]:
    bearer_token = os.environ[GENIUS_CLIENT_ACCESS_TOKEN]
    return {
        "Accept": "application/json",
        "User-Agent": "CompuServe Classic/1.22",
        "Host": "api.genius.com",
        "Authorization": f"Bearer {bearer_token}"
    }
