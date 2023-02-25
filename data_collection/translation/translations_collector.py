import asyncio
import os
import uuid
from functools import partial
from typing import List, Dict, Generator

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import IS_ISRAELI, ARTIST_NAME
from consts.microsoft_translator_consts import MICROSOFT_SUBSCRIPTION_KEY, MICROSOFT_SUBSCRIPTION_REGION, \
    MICROSOFT_TRANSLATION_LOCATION, CLIENT_TRACE_ID
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, TRANSLATIONS_PATH
from consts.rapid_api_consts import CONTENT_TYPE
from data_collection.translation.translators.microsoft_translator import MicrosoftTranslator
from tools.language_detector import LanguageDetector
from utils import to_json, chain_dicts, read_json, append_to_csv

LANGUAGE = 'language'
HEBREW_LANGAUGE_ABBREVIATION = 'he'
TRANSLATION = 'translation'


class TranslationsCollector:
    def __init__(self, chunk_size: int = 50):
        self._language_detector = LanguageDetector()
        self._chunk_size = chunk_size

    async def collect(self):
        for chunk in self._generate_artists_chunks():
            translations = await self._collect_translations(chunk)
            valid_translations = [translation for translation in translations if isinstance(translation, dict)]
            data = self._to_dataframe(valid_translations)

            append_to_csv(data=data, output_path=TRANSLATIONS_PATH)

    def _generate_artists_chunks(self) -> Generator[List[str], None, None]:
        unique_artists = self._load_artists_names()
        non_existing_artists = [artist for artist in unique_artists if artist not in self._get_existing_artists()]
        n_chunks = round(len(non_existing_artists) / self._chunk_size)

        for i in range(0, len(non_existing_artists), self._chunk_size):
            print(f'Generating chunk {self._get_chunk_number(i)} out of {n_chunks}')
            yield non_existing_artists[i: i + self._chunk_size]

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if os.path.exists(TRANSLATIONS_PATH):
            artists_translations = pd.read_csv(TRANSLATIONS_PATH, encoding=UTF_8_ENCODING)

            return artists_translations[ARTIST_NAME].tolist()

        return []

    def _get_chunk_number(self, index: int) -> int:
        chunk_number = (index / self._chunk_size) + 1
        return int(chunk_number)

    @staticmethod
    def _load_artists_names() -> List[str]:
        data = pd.read_csv(MERGED_DATA_PATH)
        israeli_data = data[data[IS_ISRAELI] == True]
        israeli_data.dropna(subset=[ARTIST_NAME], inplace=True)
        israeli_artists = israeli_data[ARTIST_NAME].unique().tolist()

        return israeli_artists

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

    @staticmethod
    def _to_dataframe(valid_translations: List[dict]) -> DataFrame:
        chained_translations = chain_dicts(valid_translations)
        data = pd.DataFrame.from_dict(chained_translations, orient='index')
        data.reset_index(level=0, inplace=True)
        data.columns = [ARTIST_NAME, TRANSLATION]

        return data


if __name__ == '__main__':
    collector = TranslationsCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())
