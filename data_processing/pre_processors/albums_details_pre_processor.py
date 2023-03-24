import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, ID, ARTIST_ID
from consts.path_consts import ARTISTS_IDS_OUTPUT_PATH, ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class AlbumsDetailsPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data_with_artists_ids = self._merge_artists_ids(data)
        return self._merge_albums_details(data_with_artists_ids)

    @staticmethod
    def _merge_artists_ids(data: DataFrame) -> DataFrame:
        artists_ids_data = pd.read_csv(ARTISTS_IDS_OUTPUT_PATH)
        return data.merge(
            how='left',
            on=ARTIST_NAME,
            right=artists_ids_data.drop(ID, axis=1)
        )

    @staticmethod
    def _merge_albums_details(data_with_artists_ids: DataFrame) -> DataFrame:
        albums_details_aggregated_data = pd.read_csv(ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH)
        return data_with_artists_ids.merge(
            how='left',
            on=ARTIST_ID,
            right=albums_details_aggregated_data
        )

    @property
    def name(self) -> str:
        return 'albums details pre processor'
