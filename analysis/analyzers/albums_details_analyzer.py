from functools import lru_cache, reduce
from typing import Dict, Tuple, List

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.aggregation_consts import MEDIAN, MAX, MIN
from consts.data_consts import ARTIST_ID, ALBUM_GROUP, ALBUM_TYPE, RELEASE_DATE, ALBUM, NAME, COUNT
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import ALBUMS_DETAILS_OUTPUT_PATH, ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH
from consts.spotify_albums_details_consts import NAMED_ALBUMS_OPTIONS, AVAILABLE_MARKETS_NUMBER, ALBUM_RELEASE_YEAR, \
    AVAILABLE_MARKETS, MEDIAN_MARKETS_NUMBER, FIRST_ALBUM_RELEASE_YEAR, LAST_ALBUM_RELEASE_YEAR, YEARS_ACTIVE
from utils.general_utils import extract_year


class AlbumsDetailsAnalyzer(IAnalyzer):
    def analyze(self) -> None:
        data = pd.read_csv(ALBUMS_DETAILS_OUTPUT_PATH)
        data.drop_duplicates(inplace=True)
        merged_aggregated_data = self._get_merged_aggregated_data(data)

        merged_aggregated_data.to_csv(ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH, index=False, encoding=UTF_8_ENCODING)

    def _get_merged_aggregated_data(self, data: DataFrame) -> DataFrame:
        aggregated_details_dfs = [
            self._get_median_markets_number(data),
            self._get_min_and_max_album_release_year(data),
            self._get_albums_count(data)
        ]
        merged_data = reduce(lambda df1, df2: df1.merge(right=df2, how='left', on=ARTIST_ID), aggregated_details_dfs)

        return merged_data

    def _get_median_markets_number(self, data: DataFrame) -> DataFrame:
        data[AVAILABLE_MARKETS_NUMBER] = data[AVAILABLE_MARKETS].apply(lambda x: self._calculate_markets_number(x))
        relevant_data = data[[ARTIST_ID, AVAILABLE_MARKETS_NUMBER]]
        grouped_data = self._groupby(relevant_data, group_by=[ARTIST_ID], agg_by=[MEDIAN])
        grouped_data.columns = [ARTIST_ID, MEDIAN_MARKETS_NUMBER]

        return grouped_data

    @staticmethod
    @lru_cache(100)
    def _calculate_markets_number(raw_available_markets: str) -> int:
        if pd.isna(raw_available_markets):
            return 0

        available_markets = eval(raw_available_markets)
        return len(available_markets)

    @staticmethod
    def _groupby(data: DataFrame, group_by: List[str], agg_by: List[str]) -> DataFrame:
        group = data.groupby(group_by).agg(agg_by)
        group.columns = agg_by
        group.reset_index(level=0, inplace=True)

        return group

    def _get_min_and_max_album_release_year(self, data: DataFrame) -> DataFrame:
        data[ALBUM_RELEASE_YEAR] = data[RELEASE_DATE].apply(lambda x: extract_year(x))
        albums_data = data[(data[ALBUM_TYPE] == ALBUM) & (data[ALBUM_GROUP] == ALBUM)]
        relevant_data = albums_data[[ARTIST_ID, ALBUM_RELEASE_YEAR]]
        grouped_data = self._groupby(relevant_data, group_by=[ARTIST_ID], agg_by=[MIN, MAX])
        grouped_data.columns = [ARTIST_ID, FIRST_ALBUM_RELEASE_YEAR, LAST_ALBUM_RELEASE_YEAR]
        grouped_data[YEARS_ACTIVE] = grouped_data[LAST_ALBUM_RELEASE_YEAR] - grouped_data[FIRST_ALBUM_RELEASE_YEAR]

        return grouped_data

    def _get_albums_count(self, data: DataFrame) -> DataFrame:
        relevant_data = data[[ARTIST_ID, ALBUM_GROUP, ALBUM_TYPE, NAME]]
        group = self._groupby(relevant_data, group_by=[ARTIST_ID, ALBUM_GROUP, ALBUM_TYPE], agg_by=[COUNT])
        unique_artists = group[ARTIST_ID].unique().tolist()
        records = []

        with tqdm(total=len(unique_artists)) as progress_bar:
            for artist_id in unique_artists:
                artist_record = self._build_artist_record(group, artist_id)
                records.append(artist_record)
                progress_bar.update(1)

        return pd.DataFrame.from_records(records)

    def _build_artist_record(self, group: DataFrame, artist_id: str) -> Dict[str, int]:
        record = {ARTIST_ID: artist_id}

        for option_name, option in NAMED_ALBUMS_OPTIONS.items():
            option_value = self._calculate_single_option_value(group, artist_id, option)
            record[option_name] = option_value

        return record

    @staticmethod
    def _calculate_single_option_value(group: DataFrame, artist_id: str, option: Tuple[str, str]) -> int:
        artist_data = group[group[ARTIST_ID] == artist_id]
        option_data = artist_data[artist_data.index == option]

        if option_data.empty:
            return 0
        else:
            return int(option_data.at[option, COUNT])

    @property
    def name(self) -> str:
        return 'albums details analyzer'


if __name__ == '__main__':
    AlbumsDetailsAnalyzer().analyze()
