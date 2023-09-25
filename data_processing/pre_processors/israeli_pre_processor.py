import json
from typing import List, Dict, Union

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from tqdm import tqdm

from consts.data_consts import IS_ISRAELI, ARTIST_NAME, NAME, MAIN_ALBUM, GENRES, ARTISTS
from consts.path_consts import KAN_GIMEL_ANALYZER_OUTPUT_PATH, SPOTIFY_ISRAELI_PLAYLISTS_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.general_utils import binary_search
from utils.regex_utils import contains_any_hebrew_character

ISRAELI = 'israeli'


class IsraeliPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        artists_mapping = self._create_israeli_artists_mapping(data)
        data[IS_ISRAELI] = data[ARTIST_NAME].map(artists_mapping)
        known_artists_data = data[~data[IS_ISRAELI].isna()]
        unknown_artists_data = data[data[IS_ISRAELI].isna()]
        unknown_artists_data[IS_ISRAELI] = self._is_israeli(unknown_artists_data)

        return pd.concat([known_artists_data, unknown_artists_data]).reset_index(drop=True)

    def _create_israeli_artists_mapping(self, data: DataFrame) -> Dict[str, Union[bool, float]]:
        print('Mapping artists to Israeli or not')
        unique_artists = data[ARTIST_NAME].unique().tolist()
        artists_mapping = {}

        with tqdm(total=len(unique_artists)) as progress_bar:
            for artist in unique_artists:
                artists_mapping[artist] = self._is_israeli_artist(artist)
                progress_bar.update(1)

        return artists_mapping

    def _is_israeli_artist(self, artist_name: str) -> Union[bool, float]:
        is_in_israeli_spotify_playlists, _ = binary_search(self._israeli_artists_from_spotify_playlists, artist_name)
        if is_in_israeli_spotify_playlists:
            return True

        if self._is_included_in_kan_gimel_data(artist_name):
            return True

        if self._contains_any_hebrew_character(artist_name):
            return True

        return np.nan

    def _is_israeli(self, data: DataFrame) -> List[bool]:
        print('Mapping unknown artists tracks to Israeli or not')
        is_israeli = []

        with tqdm(total=len(data)) as progress_bar:
            for i, row in data.iterrows():
                is_track_israeli = self._is_track_israeli(row)
                is_israeli.append(is_track_israeli)
                progress_bar.update(1)

        return is_israeli

    def _is_track_israeli(self, row: Series) -> bool:
        if self._contains_any_hebrew_character(row[NAME], row[MAIN_ALBUM]):
            return True

        else:
            return self._has_any_israeli_genre(row[GENRES])

    def _is_included_in_kan_gimel_data(self, artist_name: str) -> bool:
        return artist_name in self._kan_gimel_data[ARTISTS]

    @staticmethod
    def _contains_any_hebrew_character(*track_metadata: str) -> bool:
        for datapoint in track_metadata:
            if contains_any_hebrew_character(str(datapoint)):
                return True

        return False

    @staticmethod
    def _has_any_israeli_genre(genres: str) -> bool:
        genres_list = eval(genres)
        return any(genre.lower().__contains__(ISRAELI) for genre in genres_list)

    @property
    def _israeli_artists_from_spotify_playlists(self) -> List[str]:
        artists_data = pd.read_csv(SPOTIFY_ISRAELI_PLAYLISTS_OUTPUT_PATH)
        return artists_data[ARTIST_NAME].tolist()

    @property
    def _kan_gimel_data(self) -> Dict[str, List[str]]:
        with open(KAN_GIMEL_ANALYZER_OUTPUT_PATH, 'r') as f:
            return json.load(f)

    @property
    def name(self) -> str:
        return 'israeli pre processor'
