import os.path
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.language_consts import LANGUAGE, SCORE
from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, SHAZAM_TRACKS_LANGUAGES_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY
from tools.data_chunks_generator import DataChunksGenerator
from tools.language_detector import LanguageDetector
from utils.data_utils import extract_column_existing_values
from utils.file_utils import read_json, append_to_csv


class ShazamLyricsLanguageAnalyzer(IAnalyzer):
    def __init__(self, chunk_size: int = 50, chunks_limit: Optional[int] = None):
        self._chunk_size = chunk_size
        self._chunks_limit = chunks_limit
        self._language_detector = LanguageDetector()
        self._data_chunks_generator = DataChunksGenerator(self._chunk_size)
        self._shazam_tracks_lyrics: Dict[str, List[str]] = read_json(SHAZAM_TRACKS_LYRICS_PATH)

    def analyze(self) -> None:
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=list(self._shazam_tracks_lyrics.keys()),
            filtering_list=self._get_existing_tracks_ids()
        )

        for i, chunk in enumerate(chunks):
            if i == self._chunks_limit:
                break
            else:
                self._extract_single_chunk_tracks_languages(chunk)

    @staticmethod
    def _get_existing_tracks_ids() -> List[str]:
        tracks_keys = extract_column_existing_values(SHAZAM_TRACKS_LANGUAGES_PATH, SHAZAM_TRACK_KEY)
        return [str(track_key) for track_key in tracks_keys]

    def _extract_single_chunk_tracks_languages(self, chunk: List[str]):
        language_records = self._get_tracks_language_records(chunk)
        language_data = pd.DataFrame.from_records(language_records)

        append_to_csv(data=language_data, output_path=SHAZAM_TRACKS_LANGUAGES_PATH)

    def _get_tracks_language_records(self, chunk: List[str]) -> List[dict]:
        language_records = []

        with tqdm(total=len(chunk)) as progress_bar:
            for track_id in chunk:
                track_language_record = self._create_single_track_language_record(track_id)
                language_records.append(track_language_record)
                progress_bar.update(1)

        return language_records

    def _create_single_track_language_record(self, track_id: str) -> dict:
        track_lyrics = self._shazam_tracks_lyrics[track_id]

        if not track_lyrics:
            return {
                LANGUAGE: np.nan,
                SCORE: np.nan,
                SHAZAM_TRACK_KEY: track_id
            }

        track_concatenated_lyrics = '\n'.join(track_lyrics)
        language_record = self._language_detector.detect_language(track_concatenated_lyrics)
        language_record[SHAZAM_TRACK_KEY] = track_id

        return language_record

    @property
    def name(self) -> str:
        return 'shazam lyrics language analyzer'


if __name__ == '__main__':
    analyzer = ShazamLyricsLanguageAnalyzer()
    analyzer.analyze()
