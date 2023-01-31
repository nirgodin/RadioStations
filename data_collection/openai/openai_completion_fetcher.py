import os
import warnings
from time import sleep
from typing import List, Dict, Generator

import openai
import pandas as pd
from openai.error import ServiceUnavailableError
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, OPENAI_GENDERS_PATH

GENDER_PROMPT_FORMAT = "Given the name of a music artist, determine his or her gender of the following three options: "\
                       "'Male', 'Female' or 'Band'. In case you are not able to confidently determine the answer, " \
                       "return 'Unknown'. For example, given the following prompt 'The gender of John Lennon is', " \
                       "return 'Male'; given the following prompt 'The gender of Beyonce is', return 'Female'; Given " \
                       "The following prompt 'The gender of Pink Floyd is', return 'Band'. Your answer should include "\
                       "one token only. The gender of {} is"
OPENAI_MODEL = "text-davinci-003"
ARTIST_GENDER = 'artist_gender'


class OpenAIGenderCompletionFetcher:
    def __init__(self,
                 chunk_size: int = 50,
                 sleep_between_completions: float = 4,
                 n_retries: int = 3):
        openai.api_key = os.environ["OPENAI_API_KEY"]
        self._chunk_size = chunk_size
        self._sleep_between_completions = sleep_between_completions
        self._n_retries = n_retries

    def fetch_artists_genders(self) -> None:
        for artists_chunk in self._generate_artists_chunks():
            artists_genders = self._complete_artists_genders(artists_chunk)
            genders_data = pd.DataFrame.from_records(artists_genders)
            self._to_csv(genders_data)

    def _generate_artists_chunks(self) -> Generator[List[str], None, None]:
        data = pd.read_csv(MERGED_DATA_PATH)
        unique_artists = data[ARTIST_NAME].unique().tolist()
        non_existing_artists = [artist for artist in unique_artists if artist not in self._get_existing_artists()]
        n_chunks = round(len(non_existing_artists) / self._chunk_size)

        for i in range(0, len(non_existing_artists), self._chunk_size):
            print(f'Generating chunk {self._get_chunk_number(i)} out of {n_chunks}')
            yield non_existing_artists[i: i + self._chunk_size]

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if os.path.exists(OPENAI_GENDERS_PATH):
            genders_data = pd.read_csv(OPENAI_GENDERS_PATH)

            return genders_data[ARTIST_NAME].unique().tolist()

        return []

    def _get_chunk_number(self, index: int) -> int:
        chunk_number = (index / self._chunk_size) + 1
        return int(chunk_number)

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

        except ServiceUnavailableError:
            sleep(10)
            return self._get_artist_gender(artist_name=artist_name, retries_left=retries_left-1)

    @staticmethod
    def _create_single_completion(artist_name: str) -> str:
        response = openai.Completion.create(
            model=OPENAI_MODEL,
            prompt=GENDER_PROMPT_FORMAT.format(artist_name),
            temperature=0,
            max_tokens=7
        )

        if not response.choices:
            return ''

        first_choice = response.choices[0]
        return first_choice.text

    @staticmethod
    def _to_csv(data: DataFrame) -> None:
        if os.path.exists(OPENAI_GENDERS_PATH):
            data.to_csv(OPENAI_GENDERS_PATH, header=False, index=False, mode='a', encoding=UTF_8_ENCODING)
        else:
            data.to_csv(OPENAI_GENDERS_PATH, index=False, encoding=UTF_8_ENCODING)


if __name__ == '__main__':
    fetcher = OpenAIGenderCompletionFetcher()
    fetcher.fetch_artists_genders()
