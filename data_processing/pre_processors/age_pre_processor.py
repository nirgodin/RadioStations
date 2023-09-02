from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from consts.data_consts import AGE, IS_DEAD, ARTIST_NAME
from consts.path_consts import WIKIPEDIA_AGE_OUTPUT_PATH
from data_collection.wikipedia.wikipedia_age_collector import BIRTH_DATE, DEATH_DATE
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.datetime_utils import convert_timedelta_to_years
from consts.datetime_consts import DATETIME_FORMAT


class AgePreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        age_data = pd.read_csv(WIKIPEDIA_AGE_OUTPUT_PATH)
        age_data[AGE] = age_data[BIRTH_DATE].apply(self._calculate_age)
        age_data[IS_DEAD] = age_data[[BIRTH_DATE, DEATH_DATE]].apply(lambda x: self._is_artist_dead(*x), axis=1)

        return data.merge(
            how='left',
            on=ARTIST_NAME,
            right=age_data
        )

    @staticmethod
    def _calculate_age(birth_date: Union[str, float]) -> Union[int, float]:
        if pd.isna(birth_date):
            return np.nan

        serialized_birth_date = datetime.strptime(birth_date, DATETIME_FORMAT)
        now = datetime.now()
        age_timedelta = now - serialized_birth_date

        return convert_timedelta_to_years(age_timedelta)

    @staticmethod
    def _is_artist_dead(birth_date: Union[str, float], death_date: Union[str, float]) -> bool:
        if pd.isna(birth_date):
            return np.nan

        return not pd.isna(death_date)

    @property
    def name(self) -> str:
        return "age pre processor"
