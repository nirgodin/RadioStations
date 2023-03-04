from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, OPENAI_GENDERS_PATH, \
    WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH
from data_utils import map_df_columns, groupby_artists_by_desc_popularity


class GenderAnalyzer:
    def analyze(self):
        artists = self._get_unique_artists_names()
        records = []

        with tqdm(total=len(artists)) as progress_bar:
            for artist in artists:
                artist_record = self._extract_single_artist_gender(artist)
                records.append(artist_record)
                progress_bar.update(1)

        data = pd.DataFrame.from_records(records)
        data.to_csv(r'data/mapped_genders.csv', index=False, encoding=UTF_8_ENCODING)

    @staticmethod
    def _get_unique_artists_names() -> List[str]:
        artists_popularity = groupby_artists_by_desc_popularity()
        return artists_popularity[ARTIST_NAME].unique().tolist()

    def _extract_single_artist_gender(self, artist: str) -> Dict[str, str]:
        for source_name, source_mapping in self._sources.items():
            mapped_gender = source_mapping.get(artist)

            if mapped_gender is not None:
                return {
                    ARTIST_NAME: artist,
                    ARTIST_GENDER: mapped_gender,
                    'source': source_name
                }

        return {
            ARTIST_NAME: artist,
            ARTIST_GENDER: '',
            'source': ''
        }

    @property
    def _sources(self) -> Dict[str, Dict[str, str]]:
        return {
            'israeli_wikipedia': self._read_and_map_df_columns(WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH),
            'openai': self._read_and_map_df_columns(OPENAI_GENDERS_PATH),
            'general_wikipedia': self._read_and_map_df_columns(WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH)
        }

    @staticmethod
    def _read_and_map_df_columns(path: str) -> Dict[str, str]:
        data = pd.read_csv(path).dropna()
        data[ARTIST_GENDER] = data[ARTIST_GENDER].str.lower()
        known_data = data[~data[ARTIST_GENDER].str.contains('unknown')]

        return map_df_columns(known_data, ARTIST_NAME, ARTIST_GENDER)


if __name__ == '__main__':
    GenderAnalyzer().analyze()
