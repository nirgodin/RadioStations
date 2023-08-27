from typing import List, Dict

import pandas as pd
from pandas import DataFrame
from psmpy import PsmPy
from sklearn.impute import SimpleImputer

from consts.aggregation_consts import FIRST, MEDIAN, MAX, MIN
from consts.clustering_consts import DROPPABLE_COLUMNS, CATEGORICAL_COLUMNS, GROUPBY_FIRST_COLUMNS, \
    GROUPBY_MIN_MAX_MEDIAN_COLUMNS
from consts.data_consts import SONG, FOLLOWERS, ARTIST_NAME, DURATION_MS, IS_ISRAELI, COUNT, PLAY_COUNT
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import MERGED_DATA_PATH
from consts.spotify_albums_details_consts import YEARS_ACTIVE, SINGLES_COUNT, ALBUMS_COUNT
from utils.analsis_utils import get_artists_play_count
from utils.general_utils import chain_dicts

AGGREGATION_MAPPING = {
    DURATION_MS: MEDIAN,
    YEARS_ACTIVE: FIRST,
    SINGLES_COUNT: FIRST,
    ALBUMS_COUNT: FIRST,
    ARTIST_GENDER: FIRST,
    FOLLOWERS: MEDIAN,
    IS_ISRAELI: FIRST
}


class GenderMatcher:
    def match(self):
        data = pd.read_csv(MERGED_DATA_PATH)
        groupbyed_data = self._groupby_data(data)
        imputed_data = self._pre_process_data(groupbyed_data)
        imputed_data_with_play_count = self._add_play_count_to_data(data, imputed_data)
        psm = PsmPy(imputed_data_with_play_count, treatment=ARTIST_GENDER, indx=ARTIST_NAME, exclude=[])
        psm.logistic_ps(balance=True)
        psm.knn_matched(matcher='propensity_logit', replacement=False, caliper=None, drop_unmatched=True)

        return psm

    @staticmethod
    def _groupby_data(data: DataFrame) -> DataFrame:
        relevant_columns = list(AGGREGATION_MAPPING.keys()) + [ARTIST_NAME]
        relevant_data = data[relevant_columns]
        relevant_data = relevant_data[relevant_data[ARTIST_GENDER].isin(['male', 'female'])]
        groupbyed_data = relevant_data.groupby(ARTIST_NAME).agg(AGGREGATION_MAPPING)

        return groupbyed_data.reset_index(level=0)

    @staticmethod
    def _pre_process_data(groupbyed_data: DataFrame) -> DataFrame:
        groupbyed_data[ARTIST_GENDER] = groupbyed_data[ARTIST_GENDER].map({'male': 1, 'female': 0})
        non_artist_columns = [col for col in groupbyed_data.columns if col != ARTIST_NAME]
        non_artist_data = groupbyed_data[non_artist_columns]
        imputed_data = pd.DataFrame(
            data=SimpleImputer(strategy=MEDIAN).fit_transform(non_artist_data),
            columns=non_artist_columns
        )
        imputed_data[ARTIST_NAME] = groupbyed_data[ARTIST_NAME]

        return imputed_data

    @staticmethod
    def _add_play_count_to_data(data: DataFrame, groupbyed_data: DataFrame) -> DataFrame:
        artists_play_count = get_artists_play_count(data)
        merged_data = groupbyed_data.merge(
            right=artists_play_count,
            on=ARTIST_NAME,
            how='left'
        )
        merged_data.rename({COUNT: PLAY_COUNT}, inplace=True)

        return merged_data


if __name__ == '__main__':
    GenderMatcher().match()
