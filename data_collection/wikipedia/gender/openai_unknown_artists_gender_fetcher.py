from typing import List

import pandas as pd

from consts.data_consts import ARTIST_NAME, POPULARITY
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import OPENAI_GENDERS_PATH, WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH
from data_collection.wikipedia.gender.wikipedia_gender_fetcher import WikipediaGenderFetcher
from utils.data_utils import groupby_artists_by_desc_popularity


class OpenAIUnknownArtistsAnalyzer:
    def __init__(self, wikipedia_gender_fetcher: WikipediaGenderFetcher = WikipediaGenderFetcher()):
        self._wikipedia_gender_fetcher = wikipedia_gender_fetcher

    def analyze(self):
        artists = self._get_artists_names()
        data = self._wikipedia_gender_fetcher.fetch(artists)

        data.to_csv(WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH, index=False, encoding=UTF_8_ENCODING)

    @staticmethod
    def _get_artists_names() -> List[str]:
        data = pd.read_csv(OPENAI_GENDERS_PATH)
        unknown_artists = data[data[ARTIST_GENDER].str.contains('Unknown')]
        artists_popularity = groupby_artists_by_desc_popularity()
        unknown_artists_with_popularity = unknown_artists.merge(
            right=artists_popularity,
            on=ARTIST_NAME,
            how='left'
        )
        unknown_artists_with_popularity.sort_values(by=POPULARITY, ascending=False, inplace=True)

        return unknown_artists_with_popularity[ARTIST_NAME].unique().tolist()


if __name__ == '__main__':
    OpenAIUnknownArtistsAnalyzer().analyze()
