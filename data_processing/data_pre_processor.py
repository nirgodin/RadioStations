from typing import List, Optional

import pandas as pd
from pandas import DataFrame

from data_processing.data_merger import DataMerger
from data_processing.pre_processors.gender_per_processor import GenderPreProcessor
from data_processing.pre_processors.genre_pre_processor import GenrePreProcessor
from data_processing.pre_processors.israeli_pre_processor import IsraeliPreProcessor
from data_processing.pre_processors.pre_processor_interface import IPreProcessor

DATA_OUTPUT_PATH = r'data/merged_data.csv'


class DataPreProcessor:
    def pre_process(self, output_path: Optional[str] = None):
        data = pd.read_csv(DATA_OUTPUT_PATH)  # DataMerger.merge(output_path=DATA_OUTPUT_PATH)
        pre_processed_data = self._pre_process_data(data)

        if output_path is not None:
            pre_processed_data.to_csv(output_path, index=False)

        return pre_processed_data

    def _pre_process_data(self, data: DataFrame) -> DataFrame:
        pre_processed_data = data.copy(deep=True)

        for pre_processor in self._sorted_pre_processors:
            print(f'Starting to apply {pre_processor.name}')
            pre_processed_data = pre_processor.pre_process(pre_processed_data)

        return pre_processed_data

    @property
    def _sorted_pre_processors(self) -> List[IPreProcessor]:
        return [
            GenrePreProcessor(),
            IsraeliPreProcessor(),
            GenderPreProcessor()
        ]
