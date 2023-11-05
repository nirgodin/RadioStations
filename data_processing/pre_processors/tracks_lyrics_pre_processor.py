from typing import Tuple, Union, List, Dict

import numpy as np
from pandas import DataFrame, Series

from consts.data_consts import GENIUS_ID
from consts.lyrics_consts import SHAZAM_LYRICS, GENIUS_LYRICS, MUSIXMATCH_LYRICS, LYRICS_SOURCES, LYRICS_COLUMNS
from consts.musixmatch_consts import MUSIXMATCH_ID
from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, GENIUS_LYRICS_OUTPUT_PATH, \
    MUSIXMATCH_FORMATTED_TRACKS_LYRICS_PATH
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from models.data_source import DataSource
from utils.data_utils import is_list_na
from utils.file_utils import read_json
from utils.general_utils import stringify_float


class TracksLyricsPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data.rename(columns={f"{SHAZAM_KEY}_x": SHAZAM_KEY}, inplace=True)  # TODO: Find root problem
        data.drop(f"{SHAZAM_KEY}_y", axis=1, inplace=True)
        data_with_shazam_lyrics = self._merge_json_tracks_lyrics(
            data=data,
            path=SHAZAM_TRACKS_LYRICS_PATH,
            key_column=SHAZAM_KEY,
            lyrics_column=SHAZAM_LYRICS
        )
        data_with_genius_lyrics = self._merge_json_tracks_lyrics(
            data=data_with_shazam_lyrics,
            path=GENIUS_LYRICS_OUTPUT_PATH,
            key_column=GENIUS_ID,
            lyrics_column=GENIUS_LYRICS
        )
        data_with_lyrics = self._merge_json_tracks_lyrics(
            data=data_with_genius_lyrics,
            path=MUSIXMATCH_FORMATTED_TRACKS_LYRICS_PATH,
            key_column=MUSIXMATCH_ID,
            lyrics_column=MUSIXMATCH_LYRICS
        )
        data_with_lyrics[LYRICS_COLUMNS] = data_with_lyrics[LYRICS_SOURCES].apply(
            func=self._choose_lyrics,
            axis=1,
            result_type='expand'
        )

        return data_with_lyrics.drop(LYRICS_SOURCES, axis=1)

    @staticmethod
    def _merge_json_tracks_lyrics(data: DataFrame, path: str, key_column: str, lyrics_column: str) -> DataFrame:
        data_copy = data.copy()
        lyrics = read_json(path)
        filtered_lyrics = {k: v for k, v in lyrics.items() if v != []}
        data_copy[key_column] = data_copy[key_column].apply(stringify_float)
        data_copy[lyrics_column] = data_copy[key_column].map(filtered_lyrics)

        return data_copy

    def _choose_lyrics(self, row: Series) -> Tuple[Union[List[str], float], Union[str, float]]:
        for column, source in self._prioritized_lyrics_columns_to_sources_mapping.items():
            column_value = row[column]

            if not is_list_na(column_value):
                return column_value, source.value

        return np.nan, np.nan

    @property
    def _prioritized_lyrics_columns_to_sources_mapping(self) -> Dict[str, DataSource]:
        return {
            SHAZAM_LYRICS: DataSource.SHAZAM,
            GENIUS_LYRICS: DataSource.GENIUS,
            MUSIXMATCH_LYRICS: DataSource.MUSIXMATCH
        }

    @property
    def name(self) -> str:
        return "tracks lyrics pre processor"
