from abc import ABC, abstractmethod


class IAnalyzer(ABC):
    @abstractmethod
    def analyze(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
