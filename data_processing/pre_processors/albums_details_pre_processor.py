import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, ID, ARTIST_ID, ALBUM_ID
from consts.path_consts import ARTISTS_IDS_OUTPUT_PATH, ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH, ALBUMS_DETAILS_OUTPUT_PATH, \
    TRACKS_ALBUMS_DETAILS_OUTPUT_PATH
from consts.spotify_albums_details_consts import RAW_ALBUMS_DETAILS_RELEVANT_COLUMNS, ALBUMS_COLUMNS_RENAME_MAPPING, \
    RAW_ALBUMS_DETAILS_MERGE_COLUMNS
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class AlbumsDetailsPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        # data_with_artists_ids = self._merge_artists_ids(data)
        albums_details_raw_data = self._read_albums_details_data()
        data_with_raw_albums_details = self._merge_raw_albums_details(data_with_artists_ids, albums_details_raw_data)

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
    def _read_albums_details_data() -> DataFrame:
        artists_albums_details_raw_data = pd.read_csv(ALBUMS_DETAILS_OUTPUT_PATH)
        tracks_albums_details_raw_data = pd.read_csv(TRACKS_ALBUMS_DETAILS_OUTPUT_PATH)
        tracks_albums_details_raw_data.drop(ID, axis=1, inplace=True)
        tracks_albums_details_raw_data.rename(columns={ALBUM_ID: ID}, inplace=True)

        return pd.concat([artists_albums_details_raw_data, tracks_albums_details_raw_data])

    @staticmethod
    def _merge_raw_albums_details(data_with_artists_ids: DataFrame, albums_details_raw_data: DataFrame) -> DataFrame:
        albums_details_relevant_data = albums_details_raw_data[RAW_ALBUMS_DETAILS_RELEVANT_COLUMNS]
        albums_details_relevant_data.rename(columns=ALBUMS_COLUMNS_RENAME_MAPPING, inplace=True)
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


if __name__ == '__main__':
    AlbumsDetailsPreProcessor().pre_process('vxz')