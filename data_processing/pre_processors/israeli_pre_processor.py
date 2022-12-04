import json
from functools import lru_cache
from typing import List, Dict

from pandas import DataFrame, Series
from tqdm import tqdm

from consts.data_consts import KAN_GIMEL_ANALYZER_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor

ISRAELI = 'israeli'


class IsraeliPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data['is_israeli'] = self._is_israeli(data)
        return data

    def _is_israeli(self, data: DataFrame) -> List[bool]:
        is_israeli = []

        with tqdm(total=len(data)) as progress_bar:
            for i, row in data.iterrows():
                is_track_israeli = self._is_track_israeli(row)
                is_israeli.append(is_track_israeli)
                progress_bar.update(1)

        return is_israeli

    def _is_track_israeli(self, row: Series) -> bool:
        if self._is_included_in_kan_gimel_data(row['artist_name']):
            return True

        elif self._contains_any_hebrew_character(row['name'], row['artist_name'], row['main_album']):
            return True

        else:
            return self._has_any_israeli_genre(row['genres'])

    @lru_cache(maxsize=5000)
    def _is_included_in_kan_gimel_data(self, artist_name: str) -> bool:
        return artist_name in self._kan_gimel_data['artists']

    @staticmethod
    def _contains_any_hebrew_character(*track_metadata: str) -> bool:
        for datapoint in track_metadata:
            if any("\u0590" <= char <= "\u05EA" for char in str(datapoint)):
                return True

        return False

    @staticmethod
    def _has_any_israeli_genre(genres: str) -> bool:
        genres_list = eval(genres)
        return any(genre.lower().__contains__(ISRAELI) for genre in genres_list)

    @property
    def _kan_gimel_data(self) -> Dict[str, List[str]]:
        with open(KAN_GIMEL_ANALYZER_OUTPUT_PATH, 'r') as f:
            return json.load(f)

    @property
    def name(self) -> str:
        return 'israeli pre processor'
