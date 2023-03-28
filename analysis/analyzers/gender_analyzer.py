from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import ARTIST_NAME
from consts.gender_consts import SOURCE, ISRAELI_WIKIPEDIA, OPENAI, GENERAL_WIKIPEDIA
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, OPENAI_GENDERS_PATH, \
    WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH, MAPPED_GENDERS_OUTPUT_PATH
from data_collection.wikipedia.gender.genders import Genders
from utils.data_utils import map_df_columns, groupby_artists_by_desc_popularity


class GenderAnalyzer(IAnalyzer):
    def analyze(self) -> None:
        artists = self._get_unique_artists_names()
        records = self._get_artists_genders_records(artists)
        data = pd.DataFrame.from_records(records)

        data.to_csv(MAPPED_GENDERS_OUTPUT_PATH, index=False, encoding=UTF_8_ENCODING)

    @staticmethod
    def _get_unique_artists_names() -> List[str]:
        artists_popularity = groupby_artists_by_desc_popularity()
        return artists_popularity[ARTIST_NAME].unique().tolist()

    def _get_artists_genders_records(self, artists: List[str]) -> List[Dict[str, str]]:
        records = []

        with tqdm(total=len(artists)) as progress_bar:
            for artist in artists:
                artist_record = self._extract_single_artist_gender(artist)
                records.append(artist_record)
                progress_bar.update(1)

        return records

    def _extract_single_artist_gender(self, artist: str) -> Dict[str, str]:
        for source_name, source_mapping in self._sources.items():
            mapped_gender = source_mapping.get(artist)

            if mapped_gender is not None:
                return {
                    ARTIST_NAME: artist,
                    ARTIST_GENDER: mapped_gender,
                    SOURCE: source_name
                }

        return {
            ARTIST_NAME: artist,
            ARTIST_GENDER: '',
            SOURCE: ''
        }

    @property
    def _sources(self) -> Dict[str, Dict[str, str]]:
        return {
            ISRAELI_WIKIPEDIA: self._read_and_map_df_columns(WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH),
            OPENAI: self._read_and_map_df_columns(OPENAI_GENDERS_PATH),
            GENERAL_WIKIPEDIA: self._read_and_map_df_columns(WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH)
        }

    @staticmethod
    def _read_and_map_df_columns(path: str) -> Dict[str, str]:
        data = pd.read_csv(path).dropna()
        data[ARTIST_GENDER] = data[ARTIST_GENDER].str.lower()
        known_data = data[~data[ARTIST_GENDER].str.contains(Genders.UNKNOWN.value)]

        return map_df_columns(known_data, ARTIST_NAME, ARTIST_GENDER)

    @property
    def name(self) -> str:
        return 'gender analyzer'


if __name__ == '__main__':
    GenderAnalyzer().analyze()
