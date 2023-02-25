import os
import urllib

from requests import Response

from consts.google_translator_consts import GOOGLE_TRANSLATOR_HOST, GOOGLE_TRANSLATOR_URL, QUERY, SOURCE, TARGET, DATA, \
    TRANSLATIONS, TRANSLATED_TEXT
from consts.rapid_api_consts import RAPID_API_KEY, X_RAPID_API_HOST, X_RAPID_API_KEY
from data_collection.translation.translator_interface import ITranslator
import requests


class GoogleTranslator(ITranslator):
    def __init__(self):
        self._headers = {
            "content-type": "application/x-www-form-urlencoded",
            "Accept-Encoding": "application/gzip",
            X_RAPID_API_KEY: os.environ[RAPID_API_KEY],
            X_RAPID_API_HOST: GOOGLE_TRANSLATOR_HOST
        }

    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        raw_response = self._request(text, source_lang, target_lang)

        if not raw_response.status_code == 200:
            return ''

        return self._extract_translation(raw_response)

    def _request(self, text: str, source_lang: str, target_lang: str) -> Response:
        params = {
            QUERY: text,
            SOURCE: source_lang,
            TARGET: target_lang
        }
        payload = urllib.parse.urlencode(params)

        return requests.post(
            url=GOOGLE_TRANSLATOR_URL,
            data=payload,
            headers=self._headers
        )

    @staticmethod
    def _extract_translation(raw_response: Response) -> str:
        response = raw_response.json()
        translations = response.get(DATA, {}).get(TRANSLATIONS, [])

        if translations:
            first_translation = translations[0]
            return first_translation[TRANSLATED_TEXT]

        return ''


if __name__ == '__main__':
    translator = GoogleTranslator()
    a = translator.translate("Idan Raichel", "en", "he")
    print('b')
