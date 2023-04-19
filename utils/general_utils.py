import os
from bisect import bisect_left
from difflib import SequenceMatcher
from functools import reduce
from itertools import chain
from typing import List, Optional, Tuple, Any

import numpy as np

from component_factory import ComponentFactory
from consts.env_consts import IS_REMOTE_RUN
from consts.language_consts import LANGUAGE, HEBREW_LANGUAGE_ABBREVIATION
from consts.miscellaneous_consts import YEAR_REGEX


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


def is_remote_run() -> bool:
    return os.getenv(IS_REMOTE_RUN) == 'True'


def recursively_flatten_nested_dict(dct: dict, flatten_dct: Optional[dict] = None) -> dict:
    if flatten_dct is None:
        flatten_dct = {}

    next_dct = {}

    for k, v in dct.items():
        if isinstance(v, dict):
            next_dct.update(v)
        elif k in flatten_dct.keys():
            raise ValueError(f'Flattened dict already contains the following key `{k}`')
        else:
            flatten_dct[k] = v

    if next_dct:
        return recursively_flatten_nested_dict(dct=next_dct, flatten_dct=flatten_dct)
    else:
        return flatten_dct


def chain_lists(list_of_lists: List[list]) -> list:
    return list(chain.from_iterable(list_of_lists))


def binary_search(lst: list, value: Any) -> Tuple[bool, int]:
    index = bisect_left(lst, value)

    try:
        is_in_list = (lst[index] == value)
    except IndexError:
        is_in_list = False

    return is_in_list, index
