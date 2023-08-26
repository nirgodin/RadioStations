import pandas as pd
from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import BROADCASTING_YEAR, ARTIST_NAME, NAME, COUNT, DIFFERENCE
from consts.path_consts import MERGED_DATA_PATH, YEAR_PLAY_COUNT_DIFFERENCE_PATH
from utils.analsis_utils import get_artists_play_count
from utils.file_utils import to_csv


class YearPlayCountDifferenceAnalyzer(IAnalyzer):
    def __init__(self, baseline_year: int = 2022, alternative_year: int = 2023, use_percentage: bool = True):
        self._baseline_year = baseline_year
        self._alternative_year = alternative_year
        self._use_percentage = use_percentage

    def analyze(self) -> None:
        data = pd.read_csv(MERGED_DATA_PATH)
        baseline_play_count = self._count_songs_plays(data, self._baseline_year)
        alternative_play_count = self._count_songs_plays(data, self._alternative_year)
        comparison = self._create_comparison_data(baseline_play_count, alternative_play_count)

        to_csv(comparison, YEAR_PLAY_COUNT_DIFFERENCE_PATH)

    def _count_songs_plays(self, data: DataFrame, year: int) -> DataFrame:
        year_data = data[data[BROADCASTING_YEAR] == year]
        artists_play_count = get_artists_play_count(year_data)

        if self._use_percentage:
            total_plays = sum(artists_play_count[COUNT])
            artists_play_count[COUNT] = artists_play_count[COUNT] / total_plays * 100

        return artists_play_count.rename(columns={COUNT: f'{COUNT}_{year}'})

    def _create_comparison_data(self, baseline_play_count: DataFrame, alternative_play_count: DataFrame):
        comparison = alternative_play_count.merge(
            right=baseline_play_count,
            how='left',
            on=ARTIST_NAME
        )
        comparison.fillna(0, inplace=True)
        comparison[DIFFERENCE] = comparison[f'{COUNT}_{self._alternative_year}'] - comparison[f'{COUNT}_{self._baseline_year}']
        comparison.sort_values(by=DIFFERENCE, ascending=False, inplace=True)

        return comparison

    @property
    def name(self) -> str:
        return 'year play count difference analyzer'
