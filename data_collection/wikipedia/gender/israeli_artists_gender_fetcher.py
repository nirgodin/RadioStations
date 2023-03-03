from typing import List

import pandas as pd

from consts.data_consts import ARTIST_NAME, IS_ISRAELI
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH
from data_collection.wikipedia.gender.wikipedia_gender_fetcher import WikipediaGenderFetcher


class IsraeliArtistsGenderFetcher:
    def __init__(self, wikipedia_gender_fetcher: WikipediaGenderFetcher = WikipediaGenderFetcher()):
        self._wikipedia_gender_fetcher = wikipedia_gender_fetcher

    def fetch(self) -> None:
        israeli_artists = self._get_israeli_artists()
        data = self._wikipedia_gender_fetcher.fetch(israeli_artists)

        data.to_csv(WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, index=False, encoding=UTF_8_ENCODING)

    @staticmethod
    def _get_israeli_artists() -> List[str]:
        data = pd.read_csv(MERGED_DATA_PATH, encoding=UTF_8_ENCODING)
        israeli_artists_data = data[data[IS_ISRAELI] == True]

        return israeli_artists_data[ARTIST_NAME].unique().tolist()
