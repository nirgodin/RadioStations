from typing import List
from urllib.parse import unquote

import pandas as pd

from consts.data_consts import ARTIST_NAME, ARTIST_ID
from consts.path_consts import ARTISTS_UI_DETAILS_OUTPUT_PATH, ARTISTS_IDS_OUTPUT_PATH, WIKIPEDIA_AGE_OUTPUT_PATH
from consts.wikipedia_consts import WIKIPEDIA
from data_collection.wikipedia.age.base_wikipedia_age_collector import BaseWikipediaAgeCollector
from utils.data_utils import map_df_columns
from utils.regex_utils import search_between_two_characters


class WikipediaAgeLinkCollector(BaseWikipediaAgeCollector):
    def __init__(self):
        super().__init__()
        self._artists_ui_details_data = pd.read_csv(ARTISTS_UI_DETAILS_OUTPUT_PATH)
        self._artists_wikipedia_to_ids_mapping = map_df_columns(self._artists_ui_details_data, WIKIPEDIA, ARTIST_ID)
        self._artists_ids_data = pd.read_csv(ARTISTS_IDS_OUTPUT_PATH)
        self._artists_ids_to_names_mapping = map_df_columns(self._artists_ids_data, ARTIST_ID, ARTIST_NAME)

    def _get_contender_artists(self) -> List[str]:
        data = self._artists_ui_details_data.dropna(subset=[WIKIPEDIA])
        return data[WIKIPEDIA].tolist()

    def _get_existing_artists(self) -> List[str]:
        existing_artists_data = pd.read_csv(WIKIPEDIA_AGE_OUTPUT_PATH)
        merged_data = existing_artists_data.merge(
            right=self._artists_ids_data,
            how='left',
            on=ARTIST_NAME
        )

        return merged_data[ARTIST_ID].tolist()

    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        split_url = artist_detail.split('/')
        quoted_artist_name = split_url[-1]

        return unquote(quoted_artist_name, encoding='utf-8')

    def _get_artist_spotify_name(self, artist_detail: str) -> str:
        artist_id = self._artists_wikipedia_to_ids_mapping[artist_detail]
        return self._artists_ids_to_names_mapping[artist_id]

    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        matches = search_between_two_characters(
            start_char=r'https://',
            end_char=r'\.',
            text=artist_detail
        )
        return matches[0]
