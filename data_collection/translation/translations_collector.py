import asyncio
import os
import uuid
from functools import partial
from typing import List, Dict

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import IS_ISRAELI, ARTIST_NAME
from consts.microsoft_translator_consts import MICROSOFT_SUBSCRIPTION_KEY, MICROSOFT_SUBSCRIPTION_REGION, \
    MICROSOFT_TRANSLATION_LOCATION, CLIENT_TRACE_ID
from consts.path_consts import MERGED_DATA_PATH, TRANSLATIONS_PATH
from consts.rapid_api_consts import CONTENT_TYPE
from data_collection.translation.translators.microsoft_translator import MicrosoftTranslator
from tools.language_detector import LanguageDetector
from utils import to_json, chain_dicts

LANGUAGE = 'language'
HEBREW_LANGAUGE_ABBREVIATION = 'he'


class TranslationsCollector:
    def __init__(self):
        self._language_detector = LanguageDetector()

    async def collect(self):
        israeli_artists = self._load_artists_names()
        translations = await self._collect_translations(israeli_artists)
        valid_translations = [translation for translation in translations if isinstance(translation, dict)]
        chained_translations = chain_dicts(valid_translations)

        to_json(d=chained_translations, path=TRANSLATIONS_PATH)

    async def _collect_translations(self, israeli_artists: List[str]) -> List[dict]:
        pool = AioPool(AIO_POOL_SIZE)
        headers = {
            MICROSOFT_SUBSCRIPTION_KEY: os.environ['MICROSOFT_TRANSLATOR_KEY'],
            MICROSOFT_SUBSCRIPTION_REGION: MICROSOFT_TRANSLATION_LOCATION,
            CONTENT_TYPE: 'application/json',
            CLIENT_TRACE_ID: str(uuid.uuid4())
        }

        with tqdm(total=len(israeli_artists)) as progress_bar:
            async with ClientSession(headers=headers) as session:
                translator = MicrosoftTranslator(session)
                func = partial(self._translate_single_artist_name, translator, progress_bar)

                return await pool.map(func, israeli_artists)

    @staticmethod
    def _load_artists_names() -> List[str]:
        data = pd.read_csv(MERGED_DATA_PATH)
        israeli_data = data[data[IS_ISRAELI] == True]
        israeli_data.dropna(subset=[ARTIST_NAME], inplace=True)
        israeli_artists = israeli_data[ARTIST_NAME].unique().tolist()

        return israeli_artists[:100]

    async def _translate_single_artist_name(self, translator: MicrosoftTranslator, progress_bar: tqdm, artist_name: str) -> Dict[str, str]:
        progress_bar.update(1)
        language_and_confidence = self._language_detector.detect_language(artist_name)
        language = language_and_confidence[LANGUAGE]

        if language == HEBREW_LANGAUGE_ABBREVIATION:
            translation = artist_name
        else:
            translation = await translator.translate(
                text=artist_name,
                source_lang=language,
                target_lang=HEBREW_LANGAUGE_ABBREVIATION
            )

        return {artist_name: translation}


if __name__ == '__main__':
    collector = TranslationsCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
