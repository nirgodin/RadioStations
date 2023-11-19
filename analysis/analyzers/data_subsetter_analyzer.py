from typing import List

from analysis.analyzer_interface import IAnalyzer
from consts.audio_features_consts import ACOUSTICNESS, VALENCE
from consts.data_consts import ADDED_AT, STATION, MAIN_GENRE, IS_ISRAELI
from consts.language_consts import LANGUAGE
from utils.data_utils import read_merged_data
from utils.file_utils import to_csv


class DataSubsetterAnalyzer(IAnalyzer):
    def __init__(self, columns: List[str], output_path: str):
        self._columns = columns
        self._output_path = output_path

    def analyze(self) -> None:
        data = read_merged_data()
        subset = data[self._columns]

        to_csv(data=subset, output_path=self._output_path)

    @property
    def name(self) -> str:
        return "subsetter analyzer"


if __name__ == '__main__':
    columns = [
        ADDED_AT,
        LANGUAGE,
        STATION,
        ACOUSTICNESS,
        VALENCE,
        MAIN_GENRE,
        IS_ISRAELI,
    ]
    output_path = r"data/war/war_data.csv"
    analyzer = DataSubsetterAnalyzer(columns=columns, output_path=output_path)
    analyzer.analyze()
