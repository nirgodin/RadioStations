import pandas as pd
from pandas import DataFrame

from consts.path_consts import WIKIPEDIA_AGE_OUTPUT_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.data_utils import read_merged_data


class AgePreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        age_data = pd.read_csv(WIKIPEDIA_AGE_OUTPUT_PATH)
        print('b')

    @property
    def name(self) -> str:
        return "age pre processor"


if __name__ == '__main__':
    data = read_merged_data()
    AgePreProcessor().pre_process(data)
