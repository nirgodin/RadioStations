from typing import List, Dict

import pandas as pd

from consts.data_consts import ARTIST_NAME, IS_ISRAELI
from consts.microsoft_translator_consts import TRANSLATION
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, TRANSLATIONS_PATH
from data_collection.wikipedia.gender.wikipedia_gender_fetcher import WikipediaGenderFetcher
from data_utils import map_df_columns


class IsraeliArtistsGenderFetcher:
    def __init__(self, wikipedia_gender_fetcher: WikipediaGenderFetcher = WikipediaGenderFetcher()):
        self._wikipedia_gender_fetcher = wikipedia_gender_fetcher

    def fetch(self) -> None:
        israeli_artists = self._get_israeli_artists()
        data = self._wikipedia_gender_fetcher.fetch(israeli_artists)

        data.to_csv(WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, index=False, encoding=UTF_8_ENCODING)

    def _get_israeli_artists(self) -> List[str]:
        data = pd.read_csv(MERGED_DATA_PATH, encoding=UTF_8_ENCODING)
        israeli_artists_data = data[data[IS_ISRAELI] == True]
        artists_names = israeli_artists_data[ARTIST_NAME].unique().tolist()
        translated_artists_names = map(lambda x: self._artists_names_translations.get(x), artists_names)

        return [artist for artist in translated_artists_names if artist is not None]

    @property
    def _artists_names_translations(self) -> Dict[str, str]:
        data = pd.read_csv(TRANSLATIONS_PATH)
        return map_df_columns(data, ARTIST_NAME, TRANSLATION)


if __name__ == '__main__':
    IsraeliArtistsGenderFetcher().fetch()
