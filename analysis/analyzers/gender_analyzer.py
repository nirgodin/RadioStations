from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import ARTIST_NAME, PREDICTION_SHARE
from consts.gender_consts import SOURCE, ISRAELI_WIKIPEDIA, OPENAI, GENERAL_WIKIPEDIA, SPOTIFY_EQUAL_PLAYLISTS, \
    GOOGLE_IMAGES, MANUAL_TAGGING
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH, OPENAI_GENDERS_PATH, \
    WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH, MAPPED_GENDERS_OUTPUT_PATH, GOOGLE_IMAGES_GENDER_PATH, \
    MANUAL_GENDERS_TAGGING_PATH, SPOTIFY_ISRAEL_GLOBAL_EQUAL_PLAYLISTS_OUTPUT_PATH
from models.gender import Gender
from utils.data_utils import map_df_columns, groupby_artists_by_desc_popularity
from utils.file_utils import to_csv


class GenderAnalyzer(IAnalyzer):
    def __init__(self):
        self._prioritized_sources = {
            MANUAL_TAGGING: self._read_and_map_df_columns(MANUAL_GENDERS_TAGGING_PATH),
            SPOTIFY_EQUAL_PLAYLISTS: self._read_and_map_df_columns(SPOTIFY_ISRAEL_GLOBAL_EQUAL_PLAYLISTS_OUTPUT_PATH),
            ISRAELI_WIKIPEDIA: self._read_and_map_df_columns(WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH),
            OPENAI: self._read_and_map_df_columns(OPENAI_GENDERS_PATH),
            GENERAL_WIKIPEDIA: self._read_and_map_df_columns(WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH),
            GOOGLE_IMAGES: self._get_google_images_classifications()
        }

    def analyze(self) -> None:
        artists = self._get_unique_artists_names()
        records = self._get_artists_genders_records(artists)
        data = pd.DataFrame.from_records(records)

        to_csv(data, MAPPED_GENDERS_OUTPUT_PATH)

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
        for source_name, source_mapping in self._prioritized_sources.items():
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

    @staticmethod
    def _get_google_images_classifications() -> Dict[str, Dict[str, str]]:
        data = pd.read_csv(GOOGLE_IMAGES_GENDER_PATH).dropna()
        filtered_data = data[data[PREDICTION_SHARE] >= 0.8]

        return map_df_columns(filtered_data, ARTIST_NAME, ARTIST_GENDER)

    @staticmethod
    def _read_and_map_df_columns(path: str) -> Dict[str, str]:
        data = pd.read_csv(path).dropna()
        data[ARTIST_GENDER] = data[ARTIST_GENDER].str.lower()
        known_data = data[~data[ARTIST_GENDER].str.contains(Gender.UNKNOWN.value)]

        return map_df_columns(known_data, ARTIST_NAME, ARTIST_GENDER)

    @property
    def name(self) -> str:
        return 'gender analyzer'
