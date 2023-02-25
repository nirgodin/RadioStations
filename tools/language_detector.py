from typing import Dict, Union

import spacy
import spacy_langdetect
from spacy import Language

LANGUAGE_DETECTOR_FACTORY_KEY = "language_detector"
SPACY_ENGLISH_SMALL_MODEL = "en_core_web_sm"


class LanguageDetector:
    def detect_language(self, text: str) -> Dict[str, Union[str, float]]:
        nlp_doc = self._spacy_model(text)
        return nlp_doc._.language

    @property
    def _spacy_model(self) -> Language:
        spacy_model = spacy.load(SPACY_ENGLISH_SMALL_MODEL)
        spacy_model.add_pipe(LANGUAGE_DETECTOR_FACTORY_KEY, last=True)

        return spacy_model

    @staticmethod
    @Language.factory(LANGUAGE_DETECTOR_FACTORY_KEY)
    def __language_detector(nlp, name):
        return spacy_langdetect.LanguageDetector()
