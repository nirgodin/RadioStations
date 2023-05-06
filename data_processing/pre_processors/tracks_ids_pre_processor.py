import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, NAME, ID, URI
from consts.path_consts import MERGED_DATA_PATH, TRACKS_IDS_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class TracksIDSPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        non_missing_tracks_ids_data = data[~data[ID].isna()]
        missing_tracks_ids_data = data[data[ID].isna()]
        missing_tracks_ids_data.drop([ID, URI], axis=1, inplace=True)
        tracks_ids_data = pd.read_csv(TRACKS_IDS_OUTPUT_PATH)
        merged_data = missing_tracks_ids_data.merge(
            right=tracks_ids_data,
            how='left',
            on=[NAME, ARTIST_NAME]
        )

        return pd.concat([non_missing_tracks_ids_data, merged_data]).reset_index(drop=True)

    @property
    def name(self) -> str:
        return 'tracks ids pre processor'
