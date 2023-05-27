import os
from typing import List, Dict

import pandas as pd
from pandas import DataFrame
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer

from consts.aggregation_consts import FIRST, MEDIAN, COUNT
from consts.data_consts import SONG, URI, DURATION_MINUTES, DURATION_MS, MAJOR, MINOR
from consts.env_consts import PLAYLISTS_CREATOR_DATABASE_DRIVE_ID
from consts.path_consts import MERGED_DATA_PATH, PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH
from research.playlists_creator_database.playlists_creator_database_consts import DROPPABLE_COLUMNS, \
    GROUPBY_FIRST_COLUMNS, GROUPBY_MEDIAN_COLUMNS, LINEAR_TRANSFORMED_COLUMNS
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.drive_utils import upload_files_to_drive
from utils.general_utils import chain_dicts

ISRAELI_RADIO_PLAY_COUNT = 'israeli_radio_play_count'


class PlaylistsCreatorDatabaseGenerator:
    def generate_database(self) -> None:
        print('Starting to create PlaylistsCreator database file')
        data = pd.read_csv(MERGED_DATA_PATH)
        groubyed_data = self._groupby_data(data)
        groubyed_data.dropna(subset=[URI], inplace=True)
        pre_processed_data = self._apply_transformations(groubyed_data)
        pre_processed_data[DURATION_MINUTES] = pre_processed_data[DURATION_MS].apply(lambda x: x / (1000 * 60))
        pre_processed_data.drop(DURATION_MS, axis=1, inplace=True)

        self._output_results(pre_processed_data)

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
        linear_transformer = FunctionTransformer(self._apply_linear_transformation)
        mapper_transformer = FunctionTransformer(self._map_mode)

        preprocessor = ColumnTransformer(
            verbose_feature_names_out=False,
            remainder='passthrough',
            transformers=[
                ('linear', linear_transformer, LINEAR_TRANSFORMED_COLUMNS),
                ('map', mapper_transformer, ['mode'])
            ]
        )
        preprocessor.set_output(transform='pandas')

        return preprocessor.fit_transform(groupbyed_data)

    @staticmethod
    def _apply_linear_transformation(data: DataFrame) -> DataFrame:
        return data * 100

    @staticmethod
    def _map_mode(data: DataFrame) -> DataFrame:
        return data.applymap(lambda x: MAJOR if x == 1 else MINOR)

    @staticmethod
    def _output_results(pre_processed_data: DataFrame) -> None:
        pre_processed_data.to_csv(PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH, index=False)
        upload_metadata = GoogleDriveUploadMetadata(
            local_path=PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH,
            drive_folder_id=os.environ[PLAYLISTS_CREATOR_DATABASE_DRIVE_ID],
            file_name='groubyed_songs.csv'
        )

        upload_files_to_drive(upload_metadata)


if __name__ == '__main__':
    PlaylistsCreatorDatabaseGenerator().generate_database()
