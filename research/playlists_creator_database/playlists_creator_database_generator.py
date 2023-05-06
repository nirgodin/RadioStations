from typing import List, Dict

import pandas as pd
from pandas import DataFrame, Series
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, FunctionTransformer

from consts.aggregation_consts import FIRST, MEDIAN, MAX, MIN, SUM, COUNT
from research.playlists_creator_database.playlists_creator_database_consts import DROPPABLE_COLUMNS, \
    GROUPBY_FIRST_COLUMNS, GROUPBY_MEDIAN_COLUMNS, SCALED_COLUMNS, LINEAR_TRANSFORMED_COLUMNS
from consts.data_consts import SONG, URI, DURATION_MINUTES, DURATION_MS
from consts.path_consts import MERGED_DATA_PATH, PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH
from utils.general_utils import chain_dicts

ISRAELI_RADIO_PLAY_COUNT = 'israeli_radio_play_count'


class PlaylistsCreatorDatabaseGenerator:
    def generate_database(self) -> None:
        data = pd.read_csv(MERGED_DATA_PATH)
        groubyed_data = self._groupby_data(data)
        groubyed_data.dropna(subset=[URI], inplace=True)
        pre_processed_data = self._apply_transformations(groubyed_data)
        pre_processed_data[DURATION_MINUTES] = pre_processed_data[DURATION_MS].apply(lambda x: x / (1000 * 60))
        pre_processed_data.drop(DURATION_MS, axis=1, inplace=True)

        pre_processed_data.to_csv(PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH, index=False)

    def _groupby_data(self, data: DataFrame) -> DataFrame:
        relevant_data = data.drop(DROPPABLE_COLUMNS, axis=1)
        relevant_data[ISRAELI_RADIO_PLAY_COUNT] = relevant_data[SONG]
        aggregation_mapping = self._build_group_by_aggregation_mapping()
        groupbyed_data = relevant_data.groupby(SONG, as_index=False).agg(aggregation_mapping)
        groupbyed_data.columns = groupbyed_data.columns.droplevel(1)

        return groupbyed_data

    @staticmethod
    def _build_group_by_aggregation_mapping() -> Dict[str, List[str]]:
        first_mapping = {k: [FIRST] for k in GROUPBY_FIRST_COLUMNS}
        min_max_median_mapping = {k: [MEDIAN] for k in GROUPBY_MEDIAN_COLUMNS}
        song_count_mapping = {ISRAELI_RADIO_PLAY_COUNT: COUNT}
        mappings = [first_mapping, min_max_median_mapping, song_count_mapping]

        return chain_dicts(mappings)

    def _apply_transformations(self, groupbyed_data: DataFrame) -> DataFrame:
        minmax_transformer = Pipeline(
            steps=[
                ('minmax', MinMaxScaler()),
                ('linear', FunctionTransformer(self._apply_linear_transformation))
            ]
        )
        linear_transformer = Pipeline(
            steps=[
                ('linear', FunctionTransformer(self._apply_linear_transformation))
            ]
        )
        preprocessor = ColumnTransformer(
            verbose_feature_names_out=False,
            remainder='passthrough',
            transformers=[
                ('minmax', minmax_transformer, SCALED_COLUMNS),
                ('linear', linear_transformer, LINEAR_TRANSFORMED_COLUMNS)
            ]
        )
        preprocessor.set_output(transform='pandas')

        return preprocessor.fit_transform(groupbyed_data)

    @staticmethod
    def _apply_linear_transformation(series: Series) -> Series:
        return series * 100


if __name__ == '__main__':
    PlaylistsCreatorDatabaseGenerator().generate_database()
