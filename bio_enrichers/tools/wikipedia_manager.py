from typing import Dict, Union

from wikipediaapi import WikipediaPage

from bio_enrichers.tools.wikipedia_manager_utils import HEBREW_ABBR, get_hebrew_wikipedia, get_english_wikipedia, \
    has_any_hebrew_chars, translate_page_title


class WikipediaManager:
    def __init__(self):
        self._he_wiki = get_hebrew_wikipedia()
        self._en_wiki = get_english_wikipedia()

    def get_page_summary(self, page_title: str) -> str:
        page = self._get_page(page_title)
        if page is None:
            return ''

        return page.summary

    def _get_page(self, page_title: str) -> Union[WikipediaPage, None]:
        if has_any_hebrew_chars(page_title):
            return self._get_hebrew_page_directly(page_title)

        return self._get_hebrew_page_from_english_page(page_title)

    def _get_hebrew_page_directly(self, page_title: str) -> WikipediaPage:
        hebrew_page = self._he_wiki.page(page_title)
        if hebrew_page.exists():
            return hebrew_page

    def _get_hebrew_page_from_english_page(self, page_title: str) -> Union[WikipediaPage, None]:
        english_page = self._en_wiki.page(page_title)
        hebrew_langlink_page = self._get_hebrew_page_using_langlinks(english_page)

        if hebrew_langlink_page:
            return hebrew_langlink_page

        return self._get_hebrew_page_using_translation(page_title)

    @staticmethod
    def _get_hebrew_page_using_langlinks(english_page: WikipediaPage) -> Union[WikipediaPage, None]:
        if not english_page.exists():
            return None

        available_translated_pages: Dict[str, WikipediaPage] = english_page.langlinks
        return available_translated_pages.get(HEBREW_ABBR, None)

    def _get_hebrew_page_using_translation(self, page_title: str):
        translated_title = translate_page_title(page_title)
        if translated_title:
            hebrew_page_with_translated_title = self._he_wiki.page(translated_title)

            if hebrew_page_with_translated_title.exists():
                return hebrew_page_with_translated_title
