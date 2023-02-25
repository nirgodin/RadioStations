import os
import os
import uuid

import requests
from aiohttp import ClientSession
from requests import Response

from consts.google_translator_consts import TRANSLATIONS
from consts.microsoft_translator_consts import MICROSOFT_API_VERSION, MICROSOFT_TRANSLATION_URL, \
    MICROSOFT_TRANSLATION_LOCATION, API_VERSION, FROM, TO, CLIENT_TRACE_ID, MICROSOFT_SUBSCRIPTION_KEY, \
    MICROSOFT_SUBSCRIPTION_REGION
from consts.rapid_api_consts import CONTENT_TYPE
from consts.shazam_consts import TEXT
from data_collection.translation.translator_interface import ITranslator


class MicrosoftTranslator(ITranslator):
    def __init__(self, session: ClientSession):
        self._session = session

    async def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        response = await self._request(text, source_lang, target_lang)
        return self._extract_translation(response)

    async def _request(self, text: str, source_lang: str, target_lang: str) -> list:
        params = {
            API_VERSION: MICROSOFT_API_VERSION,
            FROM: source_lang,
            TO: [target_lang]
        }
        body = [{TEXT: text}]

        async with self._session.post(url=MICROSOFT_TRANSLATION_URL, params=params, json=body) as raw_response:
            return await raw_response.json()

    @staticmethod
    def _extract_translation(response: list) -> str:
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