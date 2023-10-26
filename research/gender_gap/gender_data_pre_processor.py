import pandas as pd
from pandas import DataFrame
from sklearn.impute import SimpleImputer

from consts.aggregation_consts import MEDIAN
from consts.data_consts import ARTIST_NAME, PLAY_COUNT, COUNT, DURATION_MS
from consts.gender_researcher_consts import AGGREGATION_MAPPING, MAIN_LANGUAGES, CATEGORICAL_COLUMNS, \
    COLINEAR_COLUMNS, SQUARED_DURATION_MS
from consts.language_consts import LANGUAGE
from consts.openai_consts import ARTIST_GENDER
from data_processing.pre_processors.genre.main_genre_mapper import OTHER
from utils.analysis_utils import aggregate_play_count
from utils.data_utils import read_merged_data


class GenderDataPreProcessor:
    def pre_process(self):
        data = read_merged_data()
        groupbyed_data = self._groupby_data(data)
        groupbyed_data[LANGUAGE] = groupbyed_data[LANGUAGE].apply(lambda x: x if x in MAIN_LANGUAGES else OTHER)
        dummies_data = pd.get_dummies(groupbyed_data, columns=CATEGORICAL_COLUMNS)
        dummies_data.drop(COLINEAR_COLUMNS, axis=1, inplace=True)
        imputed_data = self._pre_process_data(dummies_data)
        imputed_data[SQUARED_DURATION_MS] = imputed_data[DURATION_MS].apply(lambda x: int(x) ^ 2)

        return self._add_play_count_to_data(data, imputed_data)

    @staticmethod
    def _groupby_data(data: DataFrame) -> DataFrame:
        relevant_columns = list(AGGREGATION_MAPPING.keys()) + [ARTIST_NAME]
        relevant_data = data[relevant_columns]
        relevant_data.dropna(subset=[ARTIST_GENDER], inplace=True)
        groupbyed_data = relevant_data.groupby(ARTIST_NAME).agg(AGGREGATION_MAPPING)

        return groupbyed_data.reset_index(level=0)

    @staticmethod
    def _pre_process_data(groupbyed_data: DataFrame) -> DataFrame:
        non_artist_columns = [col for col in groupbyed_data.columns if col != ARTIST_NAME]
        non_artist_data = groupbyed_data[non_artist_columns]
        imputed_array = SimpleImputer(strategy=MEDIAN).fit_transform(non_artist_data)
        imputed_data = pd.DataFrame(
            data=imputed_array,
            columns=non_artist_columns
        )
        imputed_data[ARTIST_NAME] = groupbyed_data[ARTIST_NAME]

        return imputed_data

    @staticmethod
    def _add_play_count_to_data(data: DataFrame, groupbyed_data: DataFrame) -> DataFrame:
        artists_play_count = aggregate_play_count(data, column=ARTIST_NAME)
        merged_data = groupbyed_data.merge(
            right=artists_play_count,
            on=ARTIST_NAME,
            how='left'
        )
        merged_data.rename(columns={COUNT: PLAY_COUNT}, inplace=True)

        return merged_data
