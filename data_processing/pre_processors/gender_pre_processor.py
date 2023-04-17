import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_NAME
from consts.path_consts import MAPPED_GENDERS_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class GenderPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        genders_mapping = pd.read_csv(MAPPED_GENDERS_OUTPUT_PATH)
        merged_data = data.merge(
            right=genders_mapping,
            how='left',
            on=ARTIST_NAME
        )

        return merged_data

    @property
    def name(self) -> str:
        return 'gender pre processor'
