import numpy as np
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import DURATION_MINUTES, DURATION_MS
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class DurationPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        with tqdm(total=len(data)) as progress_bar:
            data[DURATION_MINUTES] = data[DURATION_MS].apply(lambda x: self._to_minutes(progress_bar, x))

        return data

    @staticmethod
    def _to_minutes(progress_bar: tqdm, duration_ms: int) -> float:
        progress_bar.update(1)

        if pd.isna(duration_ms):
            return np.nan

        return duration_ms / (1000 * 60)

    @property
    def name(self) -> str:
        return 'duration pre processor'
