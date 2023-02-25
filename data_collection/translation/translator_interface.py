from abc import ABC, abstractmethod


class ITranslator(ABC):
    @abstractmethod
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        pass
