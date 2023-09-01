from psmpy import PsmPy

from consts.data_consts import ARTIST_NAME
from consts.openai_consts import ARTIST_GENDER
from research.gender_gap.gender_data_pre_processor import GenderDataPreProcessor
from research.gender_gap.gender_researcher_linear_model import GenderResearcherLinearModel


class GenderResearcher:
    def __init__(self):
        self.data = GenderDataPreProcessor().pre_process()
        self.linear = GenderResearcherLinearModel(self.data)

    def propensity_score_matching(self) -> PsmPy:
        psm = PsmPy(self.data, treatment=ARTIST_GENDER, indx=ARTIST_NAME, exclude=[])
        psm.logistic_ps(balance=True)
        psm.knn_matched(matcher='propensity_logit', replacement=False, caliper=None, drop_unmatched=True)

        return psm


if __name__ == '__main__':
    researcher = GenderResearcher()
    comparison = researcher.linear.compare_coefficients()
    researcher.linear.plot(comparison)
