import pandas as pd
from pandas import DataFrame
from psmpy import PsmPy
from sklearn.impute import SimpleImputer

from consts.aggregation_consts import MEDIAN
from consts.data_consts import ARTIST_NAME, COUNT, PLAY_COUNT
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import MERGED_DATA_PATH
from research.gender_gap.gender_data_pre_processor import GenderDataPreProcessor
from utils.analsis_utils import get_artists_play_count
import statsmodels.api as sm


class GenderResearcher:
    def __init__(self):
        self.data = GenderDataPreProcessor().pre_process()

    def propensity_score_matching(self) -> PsmPy:
        psm = PsmPy(self.data, treatment=ARTIST_GENDER, indx=ARTIST_NAME, exclude=[])
        psm.logistic_ps(balance=True)
        psm.knn_matched(matcher='propensity_logit', replacement=False, caliper=None, drop_unmatched=True)

        return psm

    def linear_regression(self):
        x_columns = [col for col in self.data.columns if col not in [ARTIST_NAME, PLAY_COUNT]]
        y = self.data[PLAY_COUNT]
        x = sm.add_constant(self.data[x_columns])
        model = sm.OLS(y, x)

        return model.fit()


if __name__ == '__main__':
    GenderResearcher().propensity_score_matching()
