import re
from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME, IS_ISRAELI
from consts.microsoft_translator_consts import TRANSLATION
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import MERGED_DATA_PATH, TRANSLATIONS_PATH, HEBREW_WORDS_TXT_FILE_PATH_FORMAT
from data_collection.wikipedia.gender.genders import Genders
from data_collection.wikipedia.wikipedia_manager import WikipediaManager

MALE_HEBREW_WORDS_TXT_FILE_PATH = 'bio_enrichers/gender/resources/male_hebrew_words.txt'
FEMALE_HEBREW_WORDS_TXT_FILE_PATH = 'bio_enrichers/gender/resources/female_hebrew_words.txt'
BAND_HEBREW_WORDS_TXT_FILE_PATH = 'bio_enrichers/gender/resources/band_hebrew_words.txt'


class WikipediaGenderFetcher:
    def __init__(self):
        self._wikipedia_manager = WikipediaManager()
        self._male_hebrew_words = self._get_words(Genders.MALE)
        self._female_hebrew_words = self._get_words(Genders.FEMALE)
        self._band_hebrew_words = self._get_words(Genders.BAND)
        self._numeric_spaces_punctuation_regex = re.compile(r'[^\w\s]')

    def fetch(self) -> None:
        artists_records = []
        israeli_artists = self._get_israeli_artists()

        with tqdm(total=len(israeli_artists)) as progress_bar:
            for artist in israeli_artists:
                record = {
                    ARTIST_NAME: artist,
                    ARTIST_GENDER: self._find_single_artist_gender(artist)
                }
                artists_records.append(record)
                progress_bar.update(1)

        data = pd.DataFrame.from_records(artists_records)
        data.to_csv(r'data/wikipedia/israeli_artists_genders.csv', index=False, encoding=UTF_8_ENCODING)

    @staticmethod
    def _get_israeli_artists() -> List[str]:
        data = pd.read_csv(MERGED_DATA_PATH, encoding=UTF_8_ENCODING)
        israeli_artists_data = data[data[IS_ISRAELI] == True]

        return israeli_artists_data[ARTIST_NAME].unique().tolist()

    def _find_single_artist_gender(self, artist_name: str) -> str:
        translated_artist_name = self._artists_names_translations.get(artist_name)
        if translated_artist_name is None:
            return ''

        page_summary = self._wikipedia_manager.get_page_summary(page_title=translated_artist_name)
        if page_summary == '':
            return ''

        return self._extract_gender_from_page_summary(page_summary)

    def _extract_gender_from_page_summary(self, page_summary: str) -> str:
        non_punctuated_summary = re.sub(r'[^\w\s]', ' ', page_summary)
        tokenized_summary = [token.strip() for token in non_punctuated_summary.split(' ') if token != '']

        if self._contains_any_relevant_word(tokenized_summary, self._female_hebrew_words):
            return Genders.FEMALE.value

        elif self._contains_any_relevant_word(tokenized_summary, self._male_hebrew_words):
            return Genders.MALE.value

        elif self._contains_any_relevant_word(tokenized_summary, self._band_hebrew_words):
            return Genders.BAND.value

        else:
            return Genders.UNKNOWN.value

    @staticmethod
    def _get_words(gender: Genders) -> List[str]:
        path = HEBREW_WORDS_TXT_FILE_PATH_FORMAT.format(gender.value)

        with open(path, encoding='utf-8') as f:
            hebrew_words: str = f.read()

        return hebrew_words.split('\n')

    @staticmethod
    def _contains_any_relevant_word(tokenized_summary: List[str], relevant_words: List[str]) -> bool:
        return any(word in tokenized_summary for word in relevant_words)

    @property
    def _artists_names_translations(self) -> Dict[str, str]:
        data = pd.read_csv(TRANSLATIONS_PATH)
        return {
            artist_name: translation for artist_name, translation in zip(data[ARTIST_NAME], data[TRANSLATION])
        }


if __name__ == '__main__':
    gender_fetcher = WikipediaGenderFetcher()
    gender_fetcher.fetch()
