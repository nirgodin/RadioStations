from datetime import datetime, timedelta
from typing import Optional, Literal, Dict

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from analysis.analyzers.play_count_difference.play_count_difference_analyzer_config import \
    PlayCountDifferenceAnalyzerConfig
from consts.data_consts import ARTIST_NAME, COUNT, DIFFERENCE, ADDED_AT, STATION, SONG
from consts.datetime_consts import SPOTIFY_DATETIME_FORMAT
from consts.path_consts import YEAR_PLAY_COUNT_DIFFERENCE_PATH
from utils.analysis_utils import aggregate_play_count
from utils.data_utils import read_merged_data
from utils.file_utils import to_csv


class PlayCountDifferenceAnalyzer(IAnalyzer):
    def __init__(self, config: PlayCountDifferenceAnalyzerConfig):
        self._config = config

    def analyze(self) -> None:
        data = self._load_data()
        baseline_play_count = self._aggregate_play_count(data, "<")
        alternative_play_count = self._aggregate_play_count(data, ">=")
        comparison = self._create_comparison_data(baseline_play_count, alternative_play_count)

        to_csv(comparison, YEAR_PLAY_COUNT_DIFFERENCE_PATH)

    def _load_data(self) -> DataFrame:
        print("Starting to load data")
        data = read_merged_data()
        data = self._subset_interval_data(data=data, operator=">=", date_time=self._config.start_date)

        return self._subset_interval_data(data=data, operator="<", date_time=self._config.end_date)

    def _aggregate_play_count(self, data: DataFrame, operator: Literal[">=", "<"]) -> DataFrame:
        description = self._operators_descriptions[operator]
        print(f"Starting to count play songs {description} separation date")
        interval_data = self._subset_interval_data(data, operator, self._config.separation_date)
        aggregated_data = self._get_stations_aggregated_data(interval_data)

        return aggregated_data.rename(columns={COUNT: f'{COUNT}_{description}'})

    def _get_stations_aggregated_data(self, interval_data: DataFrame) -> DataFrame:
        dfs = []

        for station in interval_data[STATION].unique().tolist():
            station_play_count = self._get_single_station_aggregated_data(interval_data, station)
            dfs.append(station_play_count)

        return pd.concat(dfs)

    def _get_single_station_aggregated_data(self, interval_data: DataFrame, station: str) -> DataFrame:
        station_data = interval_data[interval_data[STATION] == station]
        play_count = aggregate_play_count(station_data, column=self._config.count_column)

        if self._config.use_percentage:
            total_plays = sum(play_count[COUNT])
            play_count[COUNT] = play_count[COUNT] / total_plays * 100

        play_count[STATION] = station

        return play_count

    @staticmethod
    def _subset_interval_data(data: DataFrame, operator: Literal[">=", "<"], date_time: Optional[datetime]) -> DataFrame:
        if date_time is None:
            return data

        formatted_date = date_time.strftime(SPOTIFY_DATETIME_FORMAT)
        query = f"`{ADDED_AT}` {operator} '{formatted_date}'"

        return data.query(query)

    def _create_comparison_data(self, baseline_play_count: DataFrame, alternative_play_count: DataFrame):
        comparison = alternative_play_count.merge(
            right=baseline_play_count,
            how='left',
            on=[self._config.count_column, STATION]
        )
        comparison.fillna(0, inplace=True)
        comparison[DIFFERENCE] = comparison[f'{COUNT}_after'] - comparison[f'{COUNT}_before']
        comparison.sort_values(by=[STATION, DIFFERENCE], ascending=False, inplace=True)

        return comparison

    @property
    def _operators_descriptions(self) -> Dict[str, str]:
        return {
            "<": "before",
            ">=": "after"
        }

    @property
    def name(self) -> str:
        return 'year play count difference analyzer'


if __name__ == '__main__':
    config = PlayCountDifferenceAnalyzerConfig(
        separation_date=datetime(2023, 10, 7, 6, 30, 0),
        start_date=datetime(2022, 1, 1, 0, 0, 0),
        count_column=SONG
    )
    analyzer = PlayCountDifferenceAnalyzer(config)
    analyzer.analyze()
