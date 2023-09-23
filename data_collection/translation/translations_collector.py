import os
import uuid
from functools import partial
from typing import List, Dict

import pandas as pd
from aiohttp import ClientSession
from asyncio_pool import AioPool
from pandas import DataFrame
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.data_consts import IS_ISRAELI, ARTIST_NAME
from consts.language_consts import HEBREW_LANGUAGE_ABBREVIATION, ENGLISH_LANGUAGE_ABBREVIATION
from consts.microsoft_translator_consts import MICROSOFT_SUBSCRIPTION_KEY, MICROSOFT_SUBSCRIPTION_REGION, \
    MICROSOFT_TRANSLATION_LOCATION, CLIENT_TRACE_ID, TRANSLATION
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import TRANSLATIONS_PATH
from consts.rapid_api_consts import CONTENT_TYPE
from data_collection.translation.translators.microsoft_translator import MicrosoftTranslator
from tools.data_chunks_generator import DataChunksGenerator
from utils.data_utils import read_merged_data
from utils.file_utils import append_to_csv
from utils.general_utils import chain_dicts, is_in_hebrew


class TranslationsCollector:
    def __init__(self, chunk_size: int = 50, max_chunks_number: int = 5):
        self._data_chunks_generator = DataChunksGenerator(chunk_size, max_chunks_number)

    async def collect(self):
        await self._data_chunks_generator.execute_by_chunk(
            lst=self._load_artists_names(),
            filtering_list=self._get_existing_artists(),
            func=self._collect_single_chunk
        )

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if os.path.exists(TRANSLATIONS_PATH):
            artists_translations = pd.read_csv(TRANSLATIONS_PATH, encoding=UTF_8_ENCODING)

            return artists_translations[ARTIST_NAME].tolist()

        return []

    @staticmethod
    def _load_artists_names() -> List[str]:
        data = read_merged_data()
        israeli_data = data[data[IS_ISRAELI] == True]
        israeli_data.dropna(subset=[ARTIST_NAME], inplace=True)
        israeli_artists = israeli_data[ARTIST_NAME].unique().tolist()

        return israeli_artists

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        translations = await self._collect_translations(chunk)
        valid_translations = [translation for translation in translations if isinstance(translation, dict)]
        data = self._to_dataframe(valid_translations)

        append_to_csv(data=data, output_path=TRANSLATIONS_PATH)

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
    async def _translate_single_artist_name(translator: MicrosoftTranslator,
                                            progress_bar: tqdm,
                                            artist_name: str) -> Dict[str, str]:
        progress_bar.update(1)

        if is_in_hebrew(artist_name):
            translation = artist_name
        else:
            translation = await translator.translate(
                text=artist_name,
                source_lang=ENGLISH_LANGUAGE_ABBREVIATION,
                target_lang=HEBREW_LANGUAGE_ABBREVIATION
            )

        return {artist_name: translation}

    @staticmethod
    def _to_dataframe(valid_translations: List[dict]) -> DataFrame:
        chained_translations = chain_dicts(valid_translations)
        data = pd.DataFrame.from_dict(chained_translations, orient='index')
        data.reset_index(level=0, inplace=True)
        data.columns = [ARTIST_NAME, TRANSLATION]

        return data
