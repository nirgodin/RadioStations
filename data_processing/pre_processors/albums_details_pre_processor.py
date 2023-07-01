import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, ID, ARTIST_ID, ALBUM_GROUP, ALBUM_TYPE, NAME, MAIN_ALBUM, ALBUM_ID
from consts.path_consts import ARTISTS_IDS_OUTPUT_PATH, ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH, ALBUMS_DETAILS_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor

RAW_ALBUMS_DETAILS_RELEVANT_COLUMNS = [
    ALBUM_GROUP,
    ALBUM_TYPE,
    ID,
    ARTIST_ID,
    NAME
]
RAW_ALBUMS_DETAILS_MERGE_COLUMNS = [ARTIST_ID, MAIN_ALBUM]


class AlbumsDetailsPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data_with_artists_ids = self._merge_artists_ids(data)
        data_with_raw_albums_details = self._merge_raw_albums_details(data_with_artists_ids)

        return self._merge_albums_details_aggregated_data(data_with_raw_albums_details)

    @staticmethod
    def _merge_artists_ids(data: DataFrame) -> DataFrame:
        artists_ids_data = pd.read_csv(ARTISTS_IDS_OUTPUT_PATH)
        return data.merge(
            how='left',
            on=ARTIST_NAME,
            right=artists_ids_data.drop(ID, axis=1)
        )

    @staticmethod
    def _merge_raw_albums_details(data_with_artists_ids: DataFrame) -> DataFrame:
        albums_details_raw_data = pd.read_csv(ALBUMS_DETAILS_OUTPUT_PATH)
        albums_details_relevant_data = albums_details_raw_data[RAW_ALBUMS_DETAILS_RELEVANT_COLUMNS]
        albums_details_relevant_data.rename(columns={NAME: MAIN_ALBUM, ID: ALBUM_ID}, inplace=True)
        albums_details_relevant_data.drop_duplicates(subset=RAW_ALBUMS_DETAILS_MERGE_COLUMNS, inplace=True)

        return data_with_artists_ids.merge(
            right=albums_details_relevant_data,
            how='left',
            on=RAW_ALBUMS_DETAILS_MERGE_COLUMNS
        )

    @staticmethod
    def _merge_albums_details_aggregated_data(data_with_raw_albums_details: DataFrame) -> DataFrame:
        albums_details_aggregated_data = pd.read_csv(ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH)
        return data_with_raw_albums_details.merge(
            how='left',
            on=ARTIST_ID,
            right=albums_details_aggregated_data
        )

    @property
    def name(self) -> str:
        return 'albums details pre processor'
