from typing import List, Optional

import pandas as pd
from pandas import DataFrame

from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, SPOTIFY_DATA_BASE_DIR
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.audio_features_pre_processor import AudioFeaturesPreProcessor
from data_processing.pre_processors.formatter_pre_processor import FormatterPreProcessor
from data_processing.pre_processors.gender_per_processor import GenderPreProcessor
from data_processing.pre_processors.genre.genre_pre_processor import GenrePreProcessor
from data_processing.pre_processors.israeli_pre_processor import IsraeliPreProcessor
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from data_processing.pre_processors.year_pre_processor import YearPreProcessor


class DataPreProcessor:
    def pre_process(self, output_path: Optional[str] = None):
        print(f'Starting to merge data to single data frame')
        data = DataMerger.merge(dir_path=SPOTIFY_DATA_BASE_DIR, output_path=MERGED_DATA_PATH)
        pre_processed_data = self._pre_process_data(data)

        if output_path is not None:
            pre_processed_data.to_csv(output_path, encoding=UTF_8_ENCODING, index=False)

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
            YearPreProcessor(),
            IsraeliPreProcessor(),
            GenrePreProcessor(),
            GenderPreProcessor(),
            AudioFeaturesPreProcessor(),
            FormatterPreProcessor()
        ]
