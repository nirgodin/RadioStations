import os
from typing import List, Dict, Optional

from pandas import DataFrame
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer

from consts.aggregation_consts import FIRST, COUNT
from consts.data_consts import SONG, URI, DURATION_MINUTES, DURATION_MS, MAJOR, MINOR, IS_REMASTERED, REMASTER
from consts.env_consts import PLAYLISTS_CREATOR_DATABASE_DRIVE_ID, PLAYLISTS_CREATOR_EMBEDDINGS_DRIVE_ID
from consts.path_consts import PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH, \
    PLAYLISTS_CREATOR_DATABASE_FILE_NAME, TRACK_NAMES_EMBEDDINGS_FILE_NAME, TRACK_NAMES_EMBEDDINGS_PATH
from research.playlists_creator_database.playlists_creator_database_consts import DROPPABLE_COLUMNS, \
    GROUPBY_FIRST_COLUMNS, GROUPBY_MIN_MAX_MEDIAN_COLUMNS, LINEAR_TRANSFORMED_COLUMNS
from tools.google_drive.google_drive_adapter import GoogleDriveAdapter
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata
from utils.data_utils import read_merged_data
from utils.general_utils import chain_dicts

ISRAELI_RADIO_PLAY_COUNT = 'israeli_radio_play_count'
REMASTERED_SEPARATOR = ' - '


class PlaylistsCreatorDatabaseGenerator:
    def __init__(self):
        self._google_drive_adapter = GoogleDriveAdapter()

    def generate_database(self) -> None:
        print('Starting to create PlaylistsCreator database file')
        data = read_merged_data()
        data[SONG] = data[[SONG, IS_REMASTERED]].apply(lambda x: self._reformat_remaster_title(*x), axis=1)
        groubyed_data = self._groupby_data(data)
        groubyed_data.dropna(subset=[URI], inplace=True)
        pre_processed_data = self._apply_transformations(groubyed_data)
        pre_processed_data[DURATION_MINUTES] = pre_processed_data[DURATION_MS].apply(lambda x: x / (1000 * 60))
        pre_processed_data.drop(DURATION_MS, axis=1, inplace=True)

        self._output_results(pre_processed_data)

    def _reformat_remaster_title(self, song_name: str, is_remastered: bool) -> str:
        if not is_remastered:
            return song_name

        song_name_components = song_name.split(REMASTERED_SEPARATOR)
        remaster_component_index = self._extract_remaster_component_index(song_name_components)

        if remaster_component_index is None:
            return song_name

        return REMASTERED_SEPARATOR.join(song_name_components[:remaster_component_index])

    @staticmethod
    def _extract_remaster_component_index(song_name_components: List[str]) -> Optional[int]:
        for i, component in enumerate(song_name_components):
            if component.lower().__contains__(REMASTER):
                return i

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
        min_max_median_mapping = {k: [v] for k, v in GROUPBY_MIN_MAX_MEDIAN_COLUMNS.items()}
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

    def _output_results(self, pre_processed_data: DataFrame) -> None:
        pre_processed_data.to_csv(PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH, index=False)
        upload_metadata = [
            GoogleDriveUploadMetadata(
                local_path=PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH,
                drive_folder_id=os.environ[PLAYLISTS_CREATOR_DATABASE_DRIVE_ID],
                file_name=PLAYLISTS_CREATOR_DATABASE_FILE_NAME
            ),
            GoogleDriveUploadMetadata(
                local_path=TRACK_NAMES_EMBEDDINGS_PATH,
                drive_folder_id=os.environ[PLAYLISTS_CREATOR_EMBEDDINGS_DRIVE_ID],
                file_name=TRACK_NAMES_EMBEDDINGS_FILE_NAME
            )
        ]

        for metadata in upload_metadata:
            self._google_drive_adapter.clean_folder(metadata.drive_folder_id)

        self._google_drive_adapter.upload(upload_metadata)
