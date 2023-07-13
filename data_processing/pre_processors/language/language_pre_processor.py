from typing import Generator

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import SPOTIFY_ID, ID
from consts.language_consts import LANGUAGE, SCORE
from consts.path_consts import SHAZAM_TRACKS_IDS_PATH, SHAZAM_TRACKS_LANGUAGES_PATH, MERGED_DATA_PATH, \
    MUSIXMATCH_TRACKS_LANGUAGES_PATH, LANGUAGES_ABBREVIATIONS_MAPPING_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY, APPLE_MUSIC_ADAM_ID
from data_processing.pre_processors.language.language_record import LanguageRecord
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.file_utils import read_json

SHAZAM_KEY = 'shazam_key'
SHAZAM_ADAMID = 'shazam_adamid'
MUSIXMATCH_LANGUAGE = 'musixmatch_language'
MUSIXMATCH_SCORE = 'musixmatch_score'
RAW_LANGUAGE_COLUMN_NAMES = [
    LANGUAGE,
    SCORE,
    MUSIXMATCH_LANGUAGE,
    MUSIXMATCH_SCORE
]


class LanguagePreProcessor(IPreProcessor):
    def __init__(self):
        self._languages_abbreviations_mapping = read_json(LANGUAGES_ABBREVIATIONS_MAPPING_PATH)

    def pre_process(self, data: DataFrame) -> DataFrame:
        data_with_shazam_ids = self._merge_shazam_tracks_ids_data(data)
        data_with_shazam_language = self._merge_lyrics_languages_data(data_with_shazam_ids)
        data_with_musixmatch_language = self._merge_musixmatch_language_data(data_with_shazam_language)
        language_data = self._create_data_with_single_language_column(data_with_musixmatch_language)
        language_data[LANGUAGE] = language_data[LANGUAGE].map(self._languages_abbreviations_mapping)

        return language_data

    @staticmethod
    def _merge_shazam_tracks_ids_data(data: DataFrame) -> DataFrame:
        shazam_tracks_ids_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        shazam_tracks_ids_relevant_data = shazam_tracks_ids_data[[SHAZAM_TRACK_KEY, APPLE_MUSIC_ADAM_ID, SPOTIFY_ID]]
        shazam_tracks_ids_relevant_data.columns = [SHAZAM_KEY, SHAZAM_ADAMID, ID]
        shazam_tracks_ids_relevant_data.drop_duplicates(subset=ID, inplace=True)

        return data.merge(
            right=shazam_tracks_ids_relevant_data,
            how='left',
            on=[ID]
        )

    @staticmethod
    def _merge_lyrics_languages_data(data_with_shazam_ids: DataFrame) -> DataFrame:
        lyrics_data = pd.read_csv(SHAZAM_TRACKS_LANGUAGES_PATH)
        lyrics_data.rename(columns={SHAZAM_TRACK_KEY: SHAZAM_KEY}, inplace=True)
        lyrics_data.drop_duplicates(subset=SHAZAM_KEY, inplace=True)

        return data_with_shazam_ids.merge(
            right=lyrics_data,
            how='left',
            on=[SHAZAM_KEY]
        )

    @staticmethod
    def _merge_musixmatch_language_data(data_with_shazam_language: DataFrame) -> DataFrame:
        musixmatch_data = pd.read_csv(MUSIXMATCH_TRACKS_LANGUAGES_PATH)
        musixmatch_data.dropna(inplace=True)
        musixmatch_data.rename(columns={LANGUAGE: MUSIXMATCH_LANGUAGE, SCORE: MUSIXMATCH_SCORE}, inplace=True)
        musixmatch_data.drop_duplicates(subset=ID, inplace=True)

        return data_with_shazam_language.merge(
            right=musixmatch_data,
            how='left',
            on=[ID]
        )

    def _create_data_with_single_language_column(self, data: DataFrame) -> DataFrame:
        unique_songs_data = data.drop_duplicates(subset=ID)
        language_records = [record.to_dict() for record in self._generate_language_data_records(unique_songs_data)]
        language_data = pd.DataFrame.from_records(language_records)
        relevant_data = data.drop(RAW_LANGUAGE_COLUMN_NAMES, axis=1)

        return relevant_data.merge(
            right=language_data,
            how='left',
            on=[ID]
        )

    @staticmethod
    def _generate_language_data_records(data: DataFrame) -> Generator[LanguageRecord, None, None]:
        print('Starting to select final language from different sources')

        with tqdm(total=len(data)) as progress_bar:
            for i, row in data.iterrows():
                if not pd.isna(row[LANGUAGE]):
                    yield LanguageRecord(
                        id=row[ID],
                        language=row[LANGUAGE],
                        score=row[SCORE],
                        lyrics_source='shazam'
                    )

                elif not pd.isna(row[MUSIXMATCH_LANGUAGE]):
                    yield LanguageRecord(
                        id=row[ID],
                        language=row[MUSIXMATCH_LANGUAGE],
                        score=row[MUSIXMATCH_SCORE],
                        lyrics_source='musixmatch'
                    )

                progress_bar.update(1)

    @property
    def name(self) -> str:
        return 'language pre processor'
