import re
import string
from collections import Counter
from typing import List, Dict, Any, Callable

import numpy as np
from pandas import DataFrame

from consts.lyrics_consts import NUMBER_OF_WORDS, WORDS_COUNT
from consts.musixmatch_consts import LYRICS
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.dict_utils import sort_dict_by_descending_value


class TracksLyricsWordsPreProcessor(IPreProcessor):
    def __init__(self):
        self._numeric_punctuation_spaces_regex = re.compile(r'[%s]' % re.escape(string.punctuation))

    def pre_process(self, data: DataFrame) -> DataFrame:
        data[WORDS_COUNT] = data[LYRICS].apply(lambda x: self._na_cleaner_wrapper(x, self._count_single_track_words))
        data[NUMBER_OF_WORDS] = data[WORDS_COUNT].apply(lambda x: self._na_cleaner_wrapper(x, self._sum_words_count))

        return data

    @staticmethod
    def _na_cleaner_wrapper(x: Any, func: Callable) -> Any:
        if not isinstance(x, (list, dict)):
            return np.nan

        return func(x)

    def _count_single_track_words(self, track_lyrics: List[str]) -> Dict[str, int]:
        track_word_count = Counter()

        for line in track_lyrics:
            clean_line = self._numeric_punctuation_spaces_regex.sub('', line)
            tokens = [token.strip().lower() for token in clean_line.split(' ') if token != '']
            tokens_count = Counter(tokens)
            track_word_count += tokens_count

        return sort_dict_by_descending_value(track_word_count)

    @staticmethod
    def _sum_words_count(words_count: Dict[str, int]) -> int:
        return sum(words_count.values())

    @property
    def name(self) -> str:
        return "lyrics words pre processor"
