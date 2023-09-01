from pandas import DataFrame

from consts.data_consts import NAME, IS_REMASTERED, REMASTER
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class RemasteredPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data[IS_REMASTERED] = data[NAME].str.contains(REMASTER, case=False)
        return data

    @property
    def name(self) -> str:
        return 'remastered pre processor'
