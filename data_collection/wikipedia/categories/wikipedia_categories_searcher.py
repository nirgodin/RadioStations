import re
from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME
from consts.microsoft_translator_consts import TRANSLATION
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, TRANSLATIONS_PATH, \
    WIKIPEDIA_RELEVANT_CATEGORIES_PATH, WIKIPEDIA_CATEGORIES_PATH
from models.gender import Gender
from data_collection.wikipedia.wikipedia_manager import WikipediaManager
from utils.file_utils import load_txt_file_lines, to_json


class WikipediaCategoriesSearcher:
    def __init__(self, wikipedia_manager: WikipediaManager = WikipediaManager()):
        self._wikipedia_manager = wikipedia_manager
        self._unique_relevant_categories = []
        self._numeric_spaces_punctuation_regex = re.compile(r'[^\w\s]')

    def fetch(self):
        artists = self._get_relevant_artists()

        with tqdm(total=len(artists)) as progress_bar:
            for artist_name in artists:
                self._fetch_single_artist_categories(artist_name)
                progress_bar.update(1)

        to_json(d=self._unique_relevant_categories, path=WIKIPEDIA_CATEGORIES_PATH)

    @staticmethod
    def _get_relevant_artists() -> List[str]:
        data = pd.read_csv(WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH)
        valid_values = [elem.value for elem in Gender if elem != Gender.UNKNOWN]
        relevant_data = data[data[ARTIST_GENDER].isin(valid_values)]

        return relevant_data[ARTIST_NAME].unique().tolist()

    def _fetch_single_artist_categories(self, artist_name: str) -> None:
        translated_artist_name = self._artists_names_translations[artist_name]
        page = self._wikipedia_manager.get_hebrew_page_directly(translated_artist_name)

        if page is None:
            return

        for category_name, category_page in page.categories.items():
            if category_name not in self._unique_relevant_categories:
                if self._is_relevant_category(category_name):
                    self._unique_relevant_categories.append(category_name)
                    print(f'Added the following category: `{category_name}`')

    def _is_relevant_category(self, category_name: str) -> bool:
        non_punctuated_category = self._numeric_spaces_punctuation_regex.sub(' ', category_name)

        for raw_token in non_punctuated_category.split(' '):
            stripped_token = raw_token.strip()

            if stripped_token in self._relevant_categories_tokens and stripped_token != '':
                return True

        return False

    @property
    def _artists_names_translations(self) -> Dict[str, str]:
        data = pd.read_csv(TRANSLATIONS_PATH)
        return {
            artist_name: translation for artist_name, translation in zip(data[ARTIST_NAME], data[TRANSLATION])
        }

    @property
    def _relevant_categories_tokens(self) -> List[str]:
        return load_txt_file_lines(WIKIPEDIA_RELEVANT_CATEGORIES_PATH)


if __name__ == '__main__':
    WikipediaCategoriesSearcher().fetch()