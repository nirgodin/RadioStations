from typing import Dict, Union, Optional

from requests import ReadTimeout
from wikipediaapi import WikipediaPage

from component_factory import ComponentFactory
from consts.language_consts import HEBREW_LANGUAGE_ABBREVIATION, ENGLISH_LANGUAGE_ABBREVIATION
from tools.language_detector import LanguageDetector
from utils.general_utils import is_in_hebrew


class WikipediaManager:
    def __init__(self):
        self._he_wiki = ComponentFactory.get_wikipedia(HEBREW_LANGUAGE_ABBREVIATION)
        self._en_wiki = ComponentFactory.get_wikipedia(ENGLISH_LANGUAGE_ABBREVIATION)
        self._language_detector = LanguageDetector()

    def get_page_summary(self, page_title: str) -> str:
        page = self._get_page(page_title)
        if page is None:
            return ''

        return page.summary

    def _get_page(self, page_title: str) -> Optional[WikipediaPage]:
        if is_in_hebrew(page_title):
            return self.get_hebrew_page_directly(page_title)

        return self._get_hebrew_page_from_english_page(page_title)

    def get_hebrew_page_directly(self, page_title: str) -> Optional[WikipediaPage]:
        try:
            hebrew_page = self._he_wiki.page(page_title)
            if hebrew_page.exists():
                return hebrew_page

        except ReadTimeout:
            print(f'Timeout on the following artist: {page_title}. Returning None')

    def _get_hebrew_page_from_english_page(self, page_title: str) -> Optional[WikipediaPage]:
        english_page = self._en_wiki.page(page_title)
        hebrew_langlink_page = self._get_hebrew_page_using_langlinks(english_page)

        if hebrew_langlink_page:
            return hebrew_langlink_page

    @staticmethod
    def _get_hebrew_page_using_langlinks(english_page: WikipediaPage) -> Optional[WikipediaPage]:
        if not english_page.exists():
            return None

        available_translated_pages: Dict[str, WikipediaPage] = english_page.langlinks
        return available_translated_pages.get(HEBREW_LANGUAGE_ABBREVIATION, None)
