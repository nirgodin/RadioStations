import json
from typing import List, Dict, Optional

import pandas as pd
from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import NAME, ARTIST_NAME, MAIN_ALBUM, STATION, COUNT
from consts.path_consts import KAN_GIMEL_ANALYZER_OUTPUT_PATH, MERGED_DATA_PATH
from consts.playlists_consts import KAN_GIMEL

COUNT_THRESHOLD = 8
EXCLUDED_ARTISTS = [
    '2Pac',
    'Dire Straits'
]


class KanGimelAnalyzer(IAnalyzer):
    def __init__(self, data_path: str = MERGED_DATA_PATH, output_path: Optional[str] = KAN_GIMEL_ANALYZER_OUTPUT_PATH):
        self._data_path = data_path
        self._output_path = output_path

    def analyze(self) -> None:
        data = pd.read_csv(self._data_path)
        kan_gimel_data = data[data[STATION] == KAN_GIMEL]
        valid_artists_names = self._extract_valid_artists_names(kan_gimel_data)
        valid_kan_gimel_data = kan_gimel_data[kan_gimel_data[ARTIST_NAME].isin(valid_artists_names)]

        self._build_output_json_data(valid_kan_gimel_data)

    def _extract_valid_artists_names(self, data: DataFrame) -> List[str]:
        artists_count = self._get_artists_play_count(data)
        valid_artists_data = artists_count[artists_count[COUNT] >= COUNT_THRESHOLD]
        valid_artists_names = valid_artists_data[ARTIST_NAME].tolist()

        return [artist for artist in valid_artists_names if artist not in EXCLUDED_ARTISTS]

    @staticmethod
    def _get_artists_play_count(data: DataFrame) -> DataFrame:
        raw_count_data = data.groupby(by=ARTIST_NAME).count().reset_index(level=0)
        count_data = raw_count_data[[ARTIST_NAME, NAME]]
        count_data.columns = [ARTIST_NAME, COUNT]

        return count_data

    def _build_output_json_data(self, data: DataFrame) -> Dict[str, List[str]]:
        jsonified_data = {
            'tracks': self._extract_unique_column_values(data, NAME),
            'artists': self._extract_unique_column_values(data, ARTIST_NAME),
            'albums': self._extract_unique_column_values(data, MAIN_ALBUM)
        }

        if self._output_path is not None:
            with open(self._output_path, 'w') as f:
                json.dump(jsonified_data, f, indent=4, ensure_ascii=True)

        return jsonified_data

    @staticmethod
    def _extract_unique_column_values(data: DataFrame, column_name: str) -> List[str]:
        return [str(value) for value in data[column_name].unique().tolist() if not pd.isna(value)]

    @property
    def name(self) -> str:
        return 'kan gimel analyzer'


if __name__ == '__main__':
    analyzer = KanGimelAnalyzer()
    analyzer.analyze()
