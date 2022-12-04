import json
from typing import List, Dict, Optional

import pandas as pd
from pandas import DataFrame

from consts.path_consts import KAN_GIMEL_ANALYZER_OUTPUT_PATH, MERGED_DATA_PATH


class KanGimelAnalyzer:
    def analyze(self, data_path: str, output_path: Optional[str] = None) -> Dict[str, List[str]]:
        data = pd.read_csv(data_path)
        kan_gimel_data = data[data['station'] == 'kan_gimel']

        return self._build_output_json_data(kan_gimel_data, output_path)

    def _build_output_json_data(self, data: DataFrame, output_path: Optional[str]) -> Dict[str, List[str]]:
        jsonified_data = {
            'tracks': self._extract_unique_column_values(data, 'name'),
            'artists': self._extract_unique_column_values(data, 'artist_name'),
            'albums': self._extract_unique_column_values(data, 'main_album')
        }

        if output_path is not None:
            with open(output_path, 'w') as f:
                json.dump(jsonified_data, f, indent=4, ensure_ascii=True)

        return jsonified_data

    @staticmethod
    def _extract_unique_column_values(data: DataFrame, column_name: str) -> List[str]:
        return [str(value) for value in data[column_name].unique().tolist() if not pd.isna(value)]


if __name__ == '__main__':
    analyzer = KanGimelAnalyzer()
    analyzer.analyze(MERGED_DATA_PATH, KAN_GIMEL_ANALYZER_OUTPUT_PATH)