import os
import re
from datetime import datetime
from functools import lru_cache
from typing import Dict, Any
import json

import numpy as np
import spotipy
from pandas import DataFrame
from spotipy import SpotifyClientCredentials

from consts.miscellaneous_consts import UTF_8_ENCODING

YEAR_REGEX = re.compile(r'.*([1-3][0-9]{3})')
JSON_ENCODING = 'utf-8'


def to_json(d: Dict[str, Any], path: str) -> None:
    with open(path, 'w', encoding=JSON_ENCODING) as f:
        json.dump(d, f, ensure_ascii=False, indent=4)


def read_json(path: str) -> dict:
    with open(path, 'r', encoding=JSON_ENCODING) as f:
        return json.load(f)


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


def append_to_csv(data: DataFrame, output_path: str) -> None:
    if os.path.exists(output_path):
        data.to_csv(output_path, header=False, index=False, mode='a', encoding=UTF_8_ENCODING)
    else:
        data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING)
