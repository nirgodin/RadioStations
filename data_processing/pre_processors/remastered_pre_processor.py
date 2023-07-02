import pandas as pd
from pandas import DataFrame

from consts.data_consts import NAME, IS_REMASTERED
from consts.path_consts import MERGED_DATA_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class RemasteredPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data[IS_REMASTERED] = data[NAME].str.contains('remaster', case=False)
        return data

    @property
    def name(self) -> str:
        return 'remastered pre processor'


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH)
    RemasteredPreProcessor().pre_process(data)
