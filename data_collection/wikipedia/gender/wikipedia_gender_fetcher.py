import re
from typing import List, Optional

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import HEBREW_WORDS_TXT_FILE_PATH_FORMAT
from models.gender import Gender
from data_collection.wikipedia.wikipedia_manager import WikipediaManager
from utils.file_utils import load_txt_file_lines


class WikipediaGenderFetcher:
    def __init__(self):
        self._wikipedia_manager = WikipediaManager()
        self._gender_words_mapping = {
            Gender.FEMALE: self._get_words(Gender.FEMALE),
            Gender.MALE: self._get_words(Gender.MALE),
            Gender.BAND: self._get_words(Gender.BAND)
        }
        self._numeric_spaces_punctuation_regex = re.compile(r'[^\w\s]')

    def fetch(self, artists: List[str]) -> DataFrame:
        artists_records = []

        with tqdm(total=len(artists)) as progress_bar:
            for artist in artists:
                gender = self._find_single_artist_gender(artist)
                print(f'{artist} gender is `{gender}`')
                record = {
                    ARTIST_NAME: artist,
                    ARTIST_GENDER: gender
                }
                artists_records.append(record)
                progress_bar.update(1)

        return pd.DataFrame.from_records(artists_records)

    def _find_single_artist_gender(self, artist_name: str) -> str:
        page_summary = self._wikipedia_manager.get_page_summary(page_title=artist_name)

        if page_summary == '':
            return ''

        return self._extract_gender_from_page_summary(page_summary)

    def _extract_gender_from_page_summary(self, page_summary: str) -> str:
        non_punctuated_summary = self._numeric_spaces_punctuation_regex.sub(' ', page_summary)

        for raw_token in non_punctuated_summary.split(' '):
            token_gender = self._extract_single_token_associated_gender(raw_token)

            if token_gender is not None:
                return token_gender

        return Gender.UNKNOWN.value

    def _extract_single_token_associated_gender(self, raw_token: str) -> Optional[str]:
        stripped_token = raw_token.strip()

        if stripped_token == '':
            return

        for gender, gender_words in self._gender_words_mapping.items():
            if stripped_token in gender_words:
                return gender.value

    @staticmethod
    def _get_words(gender: Gender) -> List[str]:
        path = HEBREW_WORDS_TXT_FILE_PATH_FORMAT.format(gender.value)
        return load_txt_file_lines(path)
