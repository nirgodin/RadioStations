import math
import os
from typing import Dict, Union, List, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import ID
from consts.language_consts import SCORE, LANGUAGE
from consts.musixmatch_consts import LYRICS_BODY
from consts.path_consts import MUSIXMATCH_TRACKS_LYRICS_PATH, MUSIXMATCH_TRACKS_LANGUAGES_PATH
from tools.data_chunks_generator import DataChunksGenerator
from tools.language_detector import LanguageDetector
from utils.file_utils import read_json, append_to_csv


class MusixmatchLyricsLanguageAnalyzer(IAnalyzer):
    def __init__(self, chunk_size: int = 50, chunks_limit: int = math.inf):
        self._chunk_size = chunk_size
        self._chunks_limit = chunks_limit
        self._tracks_lyrics = read_json(MUSIXMATCH_TRACKS_LYRICS_PATH)
        self._language_detector = LanguageDetector()
        self._data_chunks_generator = DataChunksGenerator(self._chunk_size, self._chunks_limit)

    def analyze(self) -> None:
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=list(self._tracks_lyrics.keys()),
            filtering_list=self._get_existing_tracks_ids()
        )

        for i, chunk in enumerate(chunks):
            if i == self._chunks_limit:
                break
            else:
                self._extract_single_chunk_tracks_languages(chunk)

    def _extract_single_chunk_tracks_languages(self, chunk: List[str]):
        language_records = self._get_tracks_language_records(chunk)
        language_data = pd.DataFrame.from_records(language_records)
        language_data = language_data

        append_to_csv(data=language_data, output_path=MUSIXMATCH_TRACKS_LANGUAGES_PATH)

    def _get_tracks_language_records(self, chunk: List[str]) -> List[Dict[str, Union[str, float]]]:
        records = []

        with tqdm(total=len(chunk)) as progress_bar:
            for spotify_id in chunk:
                progress_bar.update(1)
                record = self._create_track_language_record(spotify_id)

                if record is not None:
                    records.append(record)

        return records

    def _create_track_language_record(self, spotify_id: str) -> Dict[str, Union[str, float]]:
        track_lyrics = self._extract_track_lyrics(spotify_id)

        if track_lyrics == "":
            return {
                LANGUAGE: np.nan,
                SCORE: np.nan,
                ID: spotify_id
            }

        language_and_confidence = self._language_detector.detect_language(track_lyrics)
        language_and_confidence[ID] = spotify_id

        return language_and_confidence

    def _extract_track_lyrics(self, spotify_id: str) -> Optional[str]:
        return self._tracks_lyrics.get(spotify_id, {}).get(LYRICS_BODY, "")

    @staticmethod
    def _get_existing_tracks_ids() -> List[str]:
        if not os.path.exists(MUSIXMATCH_TRACKS_LANGUAGES_PATH):
            return []

        existing_data = pd.read_csv(MUSIXMATCH_TRACKS_LANGUAGES_PATH)
        return existing_data[ID].tolist()

    @property
    def name(self) -> str:
        return 'musixmatch lyrics language analyzer'


if __name__ == '__main__':
    analyzer = MusixmatchLyricsLanguageAnalyzer()
    analyzer.analyze()
