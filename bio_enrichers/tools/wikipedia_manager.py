from functools import lru_cache
from typing import Dict, Union

from wikipediaapi import Wikipedia, WikipediaPage
from google.cloud import translate_v2 as translate

HEBREW_ABBR = 'he'
ENGLISH_ABBR = 'en'


@lru_cache(maxsize=1)
def get_hebrew_wikipedia() -> Wikipedia:
    return Wikipedia(HEBREW_ABBR)


@lru_cache(maxsize=1)
def get_english_wikipedia() -> Wikipedia:
    return Wikipedia(ENGLISH_ABBR)


class WikipediaManager:
    def __init__(self):
        self._he_wiki = get_hebrew_wikipedia()
        self._en_wiki = get_english_wikipedia()
        self._translate_client = translate.Client()

    def get_page_summary(self, page_title: str) -> str:
        page = self._get_page(page_title)
        if page is None:
            return ''

        return page.summary

    def _get_page(self, page_title: str) -> Union[WikipediaPage, None]:
        hebrew_page = self._get_hebrew_page_directly(page_title)
        if hebrew_page:
            return hebrew_page

        english_page = self._en_wiki.page(page_title)
        if not english_page.exists():
            return None

        translated_hebrew_page = self._get_hebrew_page_from_english_page(english_page)
        if translated_hebrew_page:
            return translated_hebrew_page

        return english_page

    def _get_hebrew_page_directly(self, page_title: str) -> WikipediaPage:
        hebrew_page = self._he_wiki.page(page_title)
        if hebrew_page.exists():
            return hebrew_page

        translated_title = self._translate_page_title(page_title)
        if translated_title:
            hebrew_page_with_translated_title = self._he_wiki.page(translated_title)

            if hebrew_page_with_translated_title.exists():
                return hebrew_page_with_translated_title

    def _translate_page_title(self, page_title: str) -> str:
        response: Dict[str, str] = self._translate_client.translate(page_title, target_language=HEBREW_ABBR)
        if 'translatedText' in response.keys():
            return response['translatedText']

    @staticmethod
    def _get_hebrew_page_from_english_page(english_page: WikipediaPage) -> WikipediaPage:
        available_translated_pages: Dict[str, WikipediaPage] = english_page.langlinks
        return available_translated_pages.get(HEBREW_ABBR, None)
