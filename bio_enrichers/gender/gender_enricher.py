import json
from typing import List, Iterable, Dict

import pandas as pd

from bio_enrichers.gender import gender_enricher_utils
from bio_enrichers.gender.genders import Genders
from bio_enrichers.tools.wikipedia_manager import WikipediaManager
from consts.data_consts import ARTIST_NAME
from consts.openai_consts import ARTIST_GENDER
from utils import read_json

MALE_HEBREW_WORDS_TXT_FILE_PATH = 'bio_enrichers/gender/resources/male_hebrew_words.txt'
FEMALE_HEBREW_WORDS_TXT_FILE_PATH = 'bio_enrichers/gender/resources/female_hebrew_words.txt'
BAND_HEBREW_WORDS_TXT_FILE_PATH = 'bio_enrichers/gender/resources/band_hebrew_words.txt'


class GenderEnricher:
    def __init__(self):
        self._wikipedia_manager = WikipediaManager()
        self._male_hebrew_words = gender_enricher_utils.get_words(MALE_HEBREW_WORDS_TXT_FILE_PATH)
        self._female_hebrew_words = gender_enricher_utils.get_words(FEMALE_HEBREW_WORDS_TXT_FILE_PATH)
        self._band_hebrew_words = gender_enricher_utils.get_words(BAND_HEBREW_WORDS_TXT_FILE_PATH)

    def find_artists_gender(self, artists: Iterable[str]) -> Dict[str, str]:
        artists_gender = {}

        for artist in artists:
            gender = self._find_single_artist_gender(artist)
            print(f'{artist} gender is: {gender}')
            artists_gender[artist] = gender

        return artists_gender

    def _find_single_artist_gender(self, artist_name: str) -> str:
        page_summary = self._wikipedia_manager.get_page_summary(page_title=artist_name)
        if page_summary == '':
            return ''

        if gender_enricher_utils.contains_any_relevant_word(page_summary, self._female_hebrew_words):
            return Genders.FEMALE.value

        if gender_enricher_utils.contains_any_relevant_word(page_summary, self._male_hebrew_words):
            return Genders.MALE.value

        if gender_enricher_utils.contains_any_relevant_word(page_summary, self._band_hebrew_words):
            return Genders.BAND.value

        return Genders.UNKNOWN.value


if __name__ == '__main__':
    data = pd.read_csv(r'C:\Users\nirgo\Documents\GitHub\RadioStations\data\openai\artists_genders.csv', encoding='utf-8-sig')
    artists = data[data[ARTIST_GENDER].str.contains('Unknown')][ARTIST_NAME].tolist()

    gender_enricher = GenderEnricher()
    gender_enricher.find_artists_gender(artists)

