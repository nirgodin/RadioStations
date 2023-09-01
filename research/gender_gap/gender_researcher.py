from research.gender_gap.gender_data_pre_processor import GenderDataPreProcessor
from research.gender_gap.models.gender_researcher_linear_model import GenderResearcherLinearModel
from research.gender_gap.models.gender_researcher_matching_model import GenderResearcherMatchingModel


class GenderResearcher:
    def __init__(self):
        self.data = GenderDataPreProcessor().pre_process()
        self.linear = GenderResearcherLinearModel(self.data)
        self.matching = GenderResearcherMatchingModel(self.data)
