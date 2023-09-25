from functools import lru_cache

from wikipediaapi import Wikipedia

from tools.language_detector import LanguageDetector


class ComponentFactory:
    @staticmethod
    @lru_cache
    def get_language_detector() -> LanguageDetector:
        return LanguageDetector()

    @staticmethod
    @lru_cache
    def get_wikipedia(language_abbreviation: str) -> Wikipedia:
        return Wikipedia(language_abbreviation)
