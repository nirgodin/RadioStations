import re
from datetime import datetime
from functools import lru_cache
from typing import Dict, Any
import json

import numpy as np
import spotipy
from spotipy import SpotifyClientCredentials

YEAR_REGEX = re.compile(r'.*([1-3][0-9]{3})')


def to_json(d: Dict[str, Any], path: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=4)


def get_current_datetime() -> str:
    now = str(datetime.now()).replace('.', '-')
    return re.sub(r'[^\w\s]', '_', now)


@lru_cache(maxsize=1)
def get_spotipy():
    return spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


def extract_year(date: str) -> int:
    match = YEAR_REGEX.match(date)

    if match is not None:
        return int(match.group(1))

    return np.nan
