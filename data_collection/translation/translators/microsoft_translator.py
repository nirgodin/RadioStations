import os
import os
import uuid

import requests
from requests import Response

from consts.google_translator_consts import TRANSLATIONS
from consts.microsoft_translator_consts import MICROSOFT_API_VERSION, MICROSOFT_TRANSLATION_URL, \
    MICROSOFT_TRANSLATION_LOCATION, API_VERSION, FROM, TO, CLIENT_TRACE_ID, MICROSOFT_SUBSCRIPTION_KEY, \
    MICROSOFT_SUBSCRIPTION_REGION
from consts.rapid_api_consts import CONTENT_TYPE
from consts.shazam_consts import TEXT
from data_collection.translation.translator_interface import ITranslator


class MicrosoftTranslator(ITranslator):
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        raw_response = self._request(text, source_lang, target_lang)

        if raw_response.status_code != 200:
            return ''

        return self._extract_translation(raw_response)

    @staticmethod
    def _request(text: str, source_lang: str, target_lang: str) -> Response:
        params = {
            API_VERSION: MICROSOFT_API_VERSION,
            FROM: source_lang,
            TO: [target_lang]
        }
        headers = {
            MICROSOFT_SUBSCRIPTION_KEY: os.environ['MICROSOFT_TRANSLATOR_KEY'],
            MICROSOFT_SUBSCRIPTION_REGION: MICROSOFT_TRANSLATION_LOCATION,
            CONTENT_TYPE: 'application/json',
            CLIENT_TRACE_ID: str(uuid.uuid4())
        }
        body = [{TEXT: text}]

        return requests.post(
            url=MICROSOFT_TRANSLATION_URL,
            params=params,
            headers=headers,
            json=body
        )

    @staticmethod
    def _extract_translation(raw_response: Response) -> str:
        response = raw_response.json()

        if not response:
            return ''

        first_translation = response[0]
        translations = first_translation.get(TRANSLATIONS, [])

        if not translations:
            return ''

        return translations[0].get(TEXT, '')


if __name__ == '__main__':
    translator = MicrosoftTranslator()
    translator.translate('Oded Menashe', 'en', 'he')