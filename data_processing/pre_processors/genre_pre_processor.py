from pandas import DataFrame

from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class GenrePreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        return data

    @property
    def name(self) -> str:
        return 'genre pre processor'