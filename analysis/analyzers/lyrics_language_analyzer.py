import os
from typing import Dict, Union, List, Optional

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import ID
from consts.musixmatch_consts import LYRICS_BODY
from consts.path_consts import MUSIXMATCH_TRACKS_LYRICS_PATH, MUSIXMATCH_TRACKS_LANGUAGES_PATH
from tools.language_detector import LanguageDetector
from utils.file_utils import read_json

LANGUAGE_DETECTOR_FACTORY_KEY = "language_detector"
SPACY_ENGLISH_SMALL_MODEL = "en_core_web_sm"


class LyricsLanguageAnalyzer(IAnalyzer):
    def __init__(self):
        self._tracks_lyrics = read_json(MUSIXMATCH_TRACKS_LYRICS_PATH)
        self._language_detector = LanguageDetector()

    def analyze(self) -> None:
        tracks_languages_records = self._extract_tracks_languages()
        tracks_languages_data = pd.DataFrame.from_records(tracks_languages_records)

        tracks_languages_data.to_csv(MUSIXMATCH_TRACKS_LANGUAGES_PATH, index=False)

    def _extract_tracks_languages(self) -> List[Dict[str, Union[str, float]]]:
        records = []
        spotify_tracks_ids = self._tracks_lyrics.keys()

        with tqdm(total=len(spotify_tracks_ids)) as progress_bar:
            for spotify_id in spotify_tracks_ids:
                record = self._extract_single_track_language(spotify_id, progress_bar)

                if record is not None:
                    records.append(record)

        return records

    def _extract_single_track_language(self, spotify_id: str, pbar: tqdm) -> Optional[Dict[str, Union[str, float]]]:
        pbar.update(1)

        if self._is_relevant_spotify_id(spotify_id):
            return self._create_track_language_record(spotify_id)

    def _is_relevant_spotify_id(self, spotify_id: str) -> bool:
        if self._tracks_languages.empty:
            return True

        return spotify_id in self._tracks_languages[ID]

    def _create_track_language_record(self, spotify_id: str) -> Dict[str, Union[str, float]]:
        track_lyrics = self._extract_track_lyrics(spotify_id)

        if track_lyrics is None:
            return {ID: spotify_id}

        language_and_confidence = self._language_detector.detect_language(track_lyrics)
        language_and_confidence[ID] = spotify_id

        return language_and_confidence

    def _extract_track_lyrics(self, spotify_id: str) -> Optional[str]:
        return self._tracks_lyrics.get(spotify_id, {}).get(LYRICS_BODY, None)

    @property
    def _tracks_languages(self) -> DataFrame:
        if not os.path.exists(MUSIXMATCH_TRACKS_LANGUAGES_PATH):
            return pd.DataFrame()

        return pd.read_csv(MUSIXMATCH_TRACKS_LANGUAGES_PATH)

    @property
    def name(self) -> str:
        return 'lyrics language analyzer'


if __name__ == '__main__':
    analyzer = LyricsLanguageAnalyzer()
    analyzer.analyze()
