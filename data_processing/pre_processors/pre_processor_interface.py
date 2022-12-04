from abc import ABC, abstractmethod

from pandas import DataFrame


class IPreProcessor(ABC):
    @abstractmethod
    def pre_process(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
