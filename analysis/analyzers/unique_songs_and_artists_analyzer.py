from typing import Generator, Iterable

import pandas as pd
from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import STATION, ARTIST_NAME, SONG
from consts.path_consts import MERGED_DATA_PATH, UNIQUE_ANALYZER_OUTPUT_PATH_FORMAT


class UniqueAnalyzer(IAnalyzer):
    def __init__(self, columns: Iterable[str] = (SONG, ARTIST_NAME)):
        self._columns = columns

    def analyze(self) -> None:
        data = pd.read_csv(MERGED_DATA_PATH)

        for column in self._columns:
            records = list(self._generate_records(data, column))
            unique_values_data = pd.DataFrame.from_records(records)
            output_path = UNIQUE_ANALYZER_OUTPUT_PATH_FORMAT.format(column)

            unique_values_data.to_csv(output_path, index=False)

    @staticmethod
    def _generate_records(data: DataFrame, column: str) -> Generator[dict, None, None]:
        unique_stations = data[STATION].unique().tolist()

        for station in unique_stations:
            station_data = data[data[STATION] == station]
            unique_column_values = station_data[column].unique().tolist()
            unique_values_share = len(unique_column_values) / len(station_data)

            yield {
                STATION: station,
                f'unique_{column}_share': unique_values_share
            }

    @property
    def name(self) -> str:
        return 'unique songs and artists analyzer'


if __name__ == '__main__':
    unique_analyzer = UniqueAnalyzer()
    unique_analyzer.analyze()
