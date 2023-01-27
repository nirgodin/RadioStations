from pandas import DataFrame

from consts.data_consts import ADDED_AT, BROADCASTING_YEAR
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils import extract_year


class YearPreProcessor(IPreProcessor):
    def __init__(self, max_year: int = 2022):
        self._max_year = max_year

    def pre_process(self, data: DataFrame) -> DataFrame:
        data[BROADCASTING_YEAR] = [extract_year(date) for date in data[ADDED_AT]]
        filtered_data = data[data[BROADCASTING_YEAR] <= self._max_year]

        return filtered_data

    @property
    def name(self) -> str:
        return 'year pre processor'