import pandas as pd
from pandas import DataFrame

from consts.data_consts import ID
from consts.path_consts import TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class TracksIDSMapperPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        tracks_ids_mapping = pd.read_csv(TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH)
        return data.merge(
            right=tracks_ids_mapping,
            on=ID,
            how="left"
        )

    @property
    def name(self) -> str:
        return "tracks ids mapper pre processor"
