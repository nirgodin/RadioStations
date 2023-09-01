from abc import ABC, abstractmethod

from pandas import DataFrame


class IGenderResearcherModel(ABC):
    def __init__(self, data: DataFrame):
        self.data = self._pre_process_data(data)

    @abstractmethod
    def fit(self, y: str):
        raise NotImplementedError

    @abstractmethod
    def _pre_process_data(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError
