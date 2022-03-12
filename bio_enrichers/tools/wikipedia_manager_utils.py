import re
from functools import lru_cache
from typing import Dict

from wikipediaapi import Wikipedia
from google.cloud import translate_v2 as translate

HEBREW_CHAR_REGEX = re.compile(r'[\u0590-\u05fe]')

HEBREW_ABBR = 'he'
ENGLISH_ABBR = 'en'

TRANSLATED_TEXT = 'translatedText'


@lru_cache(maxsize=1)
def get_hebrew_wikipedia() -> Wikipedia:
    return Wikipedia(HEBREW_ABBR)


@lru_cache(maxsize=1)
def get_english_wikipedia() -> Wikipedia:
    return Wikipedia(ENGLISH_ABBR)


def has_any_hebrew_chars(s: str) -> bool:
    if HEBREW_CHAR_REGEX.search(s):
        return True

    return False


def translate_page_title(page_title: str) -> str:
    translate_client = _get_translate_client()
    response: Dict[str, str] = translate_client.translate(page_title, target_language=HEBREW_ABBR)
    return response.get(TRANSLATED_TEXT, None)


@lru_cache(maxsize=1)
def _get_translate_client() -> translate.Client:
    return translate.Client()
