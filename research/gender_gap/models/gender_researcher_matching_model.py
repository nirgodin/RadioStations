from typing import Union

import numpy as np
from pandas import DataFrame
from psmpy import PsmPy

from consts.data_consts import ARTIST_NAME
from consts.openai_consts import ARTIST_GENDER
from data_collection.wikipedia.gender.genders import Genders
from research.gender_gap.gender_researcher_model_interface import IGenderResearcherModel

ARTIST_GENDER_COLUMNS = [
    f"{ARTIST_GENDER}_{Genders.BAND.value}",
    f"{ARTIST_GENDER}_{Genders.MALE.value}",
]


class GenderResearcherMatchingModel(IGenderResearcherModel):
    def __init__(self, data: DataFrame):
        super().__init__(data)

    def fit(self, y: str = ARTIST_GENDER):
        psm = PsmPy(self.data, treatment=y, indx=ARTIST_NAME, exclude=[])
        psm.logistic_ps(balance=True)
        psm.knn_matched(matcher='propensity_logit', replacement=False, caliper=None, drop_unmatched=True)

        return psm

    def _pre_process_data(self, data: DataFrame) -> DataFrame:
        artist_gender_columns = [col for col in data.columns if col.startswith(ARTIST_GENDER)]
        data[ARTIST_GENDER] = data[ARTIST_GENDER_COLUMNS].apply(lambda x: self._is_male(*x), axis=1)
        data.dropna(subset=[ARTIST_GENDER], inplace=True)
        data.reset_index(drop=True)

        return data.drop(artist_gender_columns, axis=1)

    @staticmethod
    def _is_male(is_band: bool, is_male: bool) -> Union[bool, float]:
        return np.nan if is_band else is_male
