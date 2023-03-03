import re
from typing import Dict, List, Optional

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME, IS_ISRAELI
from consts.microsoft_translator_consts import TRANSLATION
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import MERGED_DATA_PATH, TRANSLATIONS_PATH, HEBREW_WORDS_TXT_FILE_PATH_FORMAT, \
    WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH
from data_collection.wikipedia.gender.genders import Genders
from data_collection.wikipedia.wikipedia_manager import WikipediaManager
from utils import load_txt_file_lines


class WikipediaGenderFetcher:
    def __init__(self):
        self._wikipedia_manager = WikipediaManager()
        self._gender_words_mapping = {
            Genders.FEMALE: self._get_words(Genders.FEMALE),
            Genders.MALE: self._get_words(Genders.MALE),
            Genders.BAND: self._get_words(Genders.BAND)
        }
        self._numeric_spaces_punctuation_regex = re.compile(r'[^\w\s]')

    def fetch(self, artists: List[str]) -> DataFrame:
        artists_records = []

        with tqdm(total=len(artists)) as progress_bar:
            for artist in artists:
                record = {
                    ARTIST_NAME: artist,
                    ARTIST_GENDER: self._find_single_artist_gender(artist)
                }
                artists_records.append(record)
                progress_bar.update(1)

        return pd.DataFrame.from_records(artists_records)

    def _find_single_artist_gender(self, artist_name: str) -> str:
        translated_artist_name = self._artists_names_translations.get(artist_name)
        if translated_artist_name is None:
            return ''

        page_summary = self._wikipedia_manager.get_page_summary(page_title=translated_artist_name)
        if page_summary == '':
            return ''

        return self._extract_gender_from_page_summary(page_summary)

    def _extract_gender_from_page_summary(self, page_summary: str) -> str:
        non_punctuated_summary = self._numeric_spaces_punctuation_regex.sub(' ', page_summary)

        for raw_token in non_punctuated_summary.split(' '):
            token_gender = self._extract_single_token_associated_gender(raw_token)

            if token_gender is not None:
                return token_gender

        return Genders.UNKNOWN.value

    def _extract_single_token_associated_gender(self, raw_token: str) -> Optional[str]:
        stripped_token = raw_token.strip()

        if stripped_token == '':
            return

        for gender, gender_words in self._gender_words_mapping.items():
            if stripped_token in gender_words:
                return gender.value

    @staticmethod
    def _get_words(gender: Genders) -> List[str]:
        path = HEBREW_WORDS_TXT_FILE_PATH_FORMAT.format(gender.value)
        return load_txt_file_lines(path)

    @property
    def _artists_names_translations(self) -> Dict[str, str]:
        data = pd.read_csv(TRANSLATIONS_PATH)
        return {
            artist_name: translation for artist_name, translation in zip(data[ARTIST_NAME], data[TRANSLATION])
        }
