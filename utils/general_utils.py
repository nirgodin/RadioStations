import re
from datetime import datetime
from difflib import SequenceMatcher
from functools import lru_cache, reduce
from typing import List

import numpy as np
import spotipy
from spotipy import SpotifyClientCredentials

from component_factory import ComponentFactory
from consts.language_consts import LANGUAGE, HEBREW_LANGUAGE_ABBREVIATION
from consts.miscellaneous_consts import YEAR_REGEX


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


def chain_dicts(dicts: List[dict]) -> dict:
    return reduce(lambda dict1, dict2: {**dict1, **dict2}, dicts)


def is_in_hebrew(s: str) -> bool:
    language_detector = ComponentFactory.get_language_detector()
    language_and_confidence = language_detector.detect_language(s)
    language = language_and_confidence[LANGUAGE]

    return language == HEBREW_LANGUAGE_ABBREVIATION


def get_similarity_score(s1: str, s2: str) -> float:
    return SequenceMatcher(None, s1, s2).ratio()