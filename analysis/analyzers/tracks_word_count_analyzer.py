import os.path
import re
from collections import Counter
from typing import Generator, List, Tuple, Dict

from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, TRACKS_WORD_COUNT_PATH
from utils.file_utils import read_json, to_json


class TracksWordCountAnalyzer(IAnalyzer):
    def __init__(self):
        self._numeric_punctuation_spaces_regex = re.compile(r'[^A-Za-z]+')

    def analyze(self) -> None:
        for chunk in self._generate_lyrics_chunks(chunk_size=50):
            chunk_word_count = self._count_chunk_words(chunk)
            self._append_to_json(chunk_word_count)

    @staticmethod
    def _generate_lyrics_chunks(chunk_size: int) -> Generator[List[Tuple[str, List[str]]], None, None]:
        lyrics_data = read_json(SHAZAM_TRACKS_LYRICS_PATH)
        lyrics_items = list(lyrics_data.items())
        n_chunks = round(len(lyrics_items) / chunk_size)

        with tqdm(total=n_chunks) as progress_bar:
            for i in range(0, len(lyrics_items), chunk_size):
                progress_bar.update(1)

                yield lyrics_items[i: i + chunk_size]

    def _count_chunk_words(self, chunk: List[Tuple[str, List[str]]]) -> Dict[str, Dict[str, int]]:
        chunk_word_count = {}

        for track_id, track_lyrics in chunk:
            track_word_count = self._count_single_track_words(track_lyrics)
            chunk_word_count[track_id] = track_word_count

        return chunk_word_count

    def _count_single_track_words(self, track_lyrics: List[str]) -> Dict[str, int]:
        track_word_count = Counter()

        for line in track_lyrics:
            clean_line = self._numeric_punctuation_spaces_regex.sub(line, ' ')
            tokens = [token.strip() for token in clean_line.split(' ') if token != '']
            tokens_count = Counter(tokens)
            track_word_count += tokens_count

        return dict(track_word_count)

    @staticmethod
    def _append_to_json(chunk_word_count: Dict[str, Dict[str, int]]) -> None:
        if os.path.exists(TRACKS_WORD_COUNT_PATH):
            existing_word_count = read_json(TRACKS_WORD_COUNT_PATH)
            chunk_word_count.update(existing_word_count)

        to_json(d=chunk_word_count, path=TRACKS_WORD_COUNT_PATH)

    @property
    def name(self) -> str:
        return 'tracks word count analyzer'


if __name__ == '__main__':
    analyzer = TracksWordCountAnalyzer()
    analyzer.analyze()
