import re
from typing import Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from consts.audio_features_consts import AUDIO_FEATURES, KEY
from consts.data_consts import POPULARITY, DURATION_MS, EXPLICIT, IS_ISRAELI, ID, \
    TRACK_NUMBER, RELEASE_DATE, MAIN_GENRE
from consts.path_consts import MERGED_DATA_PATH, CORRELATIONS_DATA_PATH
from utils import extract_year

RELEASE_YEAR = 'release_year'
DUMMY_COLUMNS = [
    KEY,
    MAIN_GENRE
]

PLAYS_COUNT = 'play_count'
CORRELATION_COLUMNS_SUBSET = AUDIO_FEATURES + [
    POPULARITY,
    EXPLICIT,
    DURATION_MS,
    IS_ISRAELI,
    TRACK_NUMBER,
    RELEASE_DATE,
    ID,
    MAIN_GENRE
]
CORRELATION = 'correlation'
X = 'x'
Y = 'y'


class AudioFeaturesCorrelationsAnalyzer:
    def analyze(self):
        data = self._get_clean_data()
        audio_features_and_popularity = data[CORRELATION_COLUMNS_SUBSET]
        enriched_data = self._build_features(audio_features_and_popularity)
        correlations = self._get_correlations(enriched_data)
        correlations = correlations.applymap(lambda x: self._format(x))

        correlations.to_csv(CORRELATIONS_DATA_PATH, index=False)

    @staticmethod
    def _get_clean_data() -> DataFrame:
        merged_data = pd.read_csv(MERGED_DATA_PATH)
        merged_data.dropna(subset=AUDIO_FEATURES, inplace=True)

        return merged_data

    def _build_features(self, data: DataFrame) -> DataFrame:
        data[RELEASE_YEAR] = [extract_year(date) for date in data[RELEASE_DATE]]
        data_with_play_count = data.merge(
            right=self._get_tracks_play_count(data),
            how='left',
            on=ID
        )

        return pd.get_dummies(data_with_play_count, columns=DUMMY_COLUMNS)

    @staticmethod
    def _get_tracks_play_count(data: DataFrame) -> DataFrame:
        groubyed_data = data.groupby(by=[ID]).count()
        first_column_name = groubyed_data.columns.tolist()[0]
        subseted_groupbyed_data = groubyed_data[[first_column_name]]
        subseted_groupbyed_data.reset_index(level=0, inplace=True)
        subseted_groupbyed_data.columns = [ID, PLAYS_COUNT]

        return subseted_groupbyed_data

    def _get_correlations(self, data: DataFrame) -> DataFrame:
        popularity_correlations = self._get_column_correlations(data, POPULARITY)
        play_count_correlations = self._get_column_correlations(data, PLAYS_COUNT)

        return pd.concat([popularity_correlations, play_count_correlations])

    @staticmethod
    def _get_column_correlations(data: DataFrame, column: str) -> DataFrame:
        raw_correlations = data.corr()
        column_correlations = raw_correlations[column].to_frame()
        column_correlations.drop(column, axis=0, inplace=True)
        column_correlations.reset_index(level=0, inplace=True)
        column_correlations.columns = [X, CORRELATION]
        column_correlations[Y] = column

        return column_correlations.sort_values(by=CORRELATION, ascending=False)

    @staticmethod
    def _format(value: Union[float, str]):
        if not isinstance(value, str):
            return value

        tokenized_value = value.split('_')
        titlized_value = [token.title() for token in tokenized_value]

        return ' '.join(titlized_value)


if __name__ == '__main__':
    AudioFeaturesCorrelationsAnalyzer().analyze()