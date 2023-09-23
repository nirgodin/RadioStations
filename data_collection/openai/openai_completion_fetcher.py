import os
import warnings
from time import sleep
from typing import List, Dict, Generator

import openai
import pandas as pd
from openai.error import ServiceUnavailableError, RateLimitError, APIError
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME
from consts.openai_consts import GENDER_PROMPT_FORMAT, OPENAI_MODEL, ARTIST_GENDER
from consts.path_consts import OPENAI_GENDERS_PATH
from tools.data_chunks_generator import DataChunksGenerator
from utils.data_utils import groupby_artists_by_desc_popularity
from utils.file_utils import append_to_csv


class OpenAIGenderCompletionFetcher:
    def __init__(self,
                 chunk_size: int = 50,
                 max_chunks_numer: int = 5,
                 sleep_between_completions: float = 4,
                 n_retries: int = 3,
                 model: str = OPENAI_MODEL):
        openai.api_key = os.environ["TOM_API_KEY"]
        self._sleep_between_completions = sleep_between_completions
        self._n_retries = n_retries
        self._model = model
        self._data_chunks_generator = DataChunksGenerator(chunk_size, max_chunks_numer)

    def fetch_artists_genders(self) -> None:
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=self._get_unique_artists_by_desc_popularity(),
            filtering_list=self._get_existing_artists()
        )

        for artists_chunk in chunks:
            artists_genders = self._complete_artists_genders(artists_chunk)
            genders_data = pd.DataFrame.from_records(artists_genders)
            append_to_csv(data=genders_data, output_path=OPENAI_GENDERS_PATH)

    @staticmethod
    def _get_unique_artists_by_desc_popularity() -> List[str]:
        artists_mean_popularity = groupby_artists_by_desc_popularity()
        return artists_mean_popularity[ARTIST_NAME].unique().tolist()

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if os.path.exists(OPENAI_GENDERS_PATH):
            genders_data = pd.read_csv(OPENAI_GENDERS_PATH)

            return genders_data[ARTIST_NAME].unique().tolist()

        return []

    def _complete_artists_genders(self, artist_names: List[str]) -> List[Dict[str, str]]:
        records = []

        with tqdm(total=self._chunk_size) as progress_bar:
            for artist_name in artist_names:
                record = self._complete_single_artist_gender(artist_name=artist_name, progress_bar=progress_bar)
                records.append(record)

        return records

    def _complete_single_artist_gender(self, artist_name: str, progress_bar: tqdm) -> Dict[str, str]:
        artist_gender = self._get_artist_gender(artist_name=artist_name, retries_left=self._n_retries)
        record = {
            ARTIST_NAME: artist_name,
            ARTIST_GENDER: artist_gender
        }
        sleep(self._sleep_between_completions)
        progress_bar.update(1)

        return record

    def _get_artist_gender(self, artist_name: str, retries_left: int) -> str:
        if retries_left == 0:
            warnings.warn(f'Could not fetch gender for `{artist_name}`. Returning empty string instead')
            return ''

        try:
            return self._create_single_completion(artist_name)

        except (ServiceUnavailableError, RateLimitError, APIError):
            sleep(10)
            print('Received exception! Retrying')
            return self._get_artist_gender(artist_name=artist_name, retries_left=retries_left-1)

    def _create_single_completion(self, artist_name: str) -> str:
        response = openai.Completion.create(
            model=self._model,
            prompt=GENDER_PROMPT_FORMAT.format(artist_name),
            temperature=0,
            max_tokens=7
        )

        if not response.choices:
            return ''

        first_choice = response.choices[0]
        return first_choice.text


if __name__ == '__main__':
    fetcher = OpenAIGenderCompletionFetcher()
    fetcher.fetch_artists_genders()
