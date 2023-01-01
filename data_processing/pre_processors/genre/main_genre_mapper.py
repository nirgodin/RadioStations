from typing import Dict, List, Optional

import pandas as pd
from nltk import MWETokenizer

from consts.data_consts import GENRE, MAIN_GENRE
from consts.path_consts import GENRES_MAPPING_PATH

OTHER = 'other'
TOKENIZER_SEPARATOR = ' '


class MainGenreMapper:
    def map(self, genre: str) -> str:
        if self._is_genre_tagged(genre):
            return self._tagged_genres[genre]

        contained_main_genre = self._extract_main_genre_if_contained(genre)
        if contained_main_genre is not None:
            return contained_main_genre

        return OTHER

    def _is_genre_tagged(self, genre: str) -> bool:
        return genre in self._tagged_genres.keys()

    def _extract_main_genre_if_contained(self, genre: str) -> Optional[str]:
        raw_tokens = genre.split(TOKENIZER_SEPARATOR)
        tokenized_genre = self._tokenizer.tokenize(raw_tokens)

        for main_genre in self._main_genres:
            for token in tokenized_genre:
                if token == main_genre:
                    return main_genre

    @property
    def _tagged_genres(self) -> Dict[str, str]:
        genres_data = pd.read_csv(GENRES_MAPPING_PATH).fillna('')
        genres_mapping = {}

        for genre, main_genre in zip(genres_data[GENRE], genres_data[MAIN_GENRE]):
            if main_genre != '':
                genres_mapping[genre] = main_genre

        return genres_mapping

    @property
    def _main_genres(self) -> List[str]:
        main_genres = set(self._tagged_genres.values())
        return list(sorted(main_genres))

    @property
    def _tokenizer(self) -> MWETokenizer:
        tokenizer = MWETokenizer(separator=TOKENIZER_SEPARATOR)

        for genre in self._main_genres:
            if self._is_multi_word_expression(genre):
                tokenizer_formatted_genre = tuple(genre.split(TOKENIZER_SEPARATOR))
                tokenizer.add_mwe(tokenizer_formatted_genre)

        return tokenizer

    @staticmethod
    def _is_multi_word_expression(s: str) -> bool:
        return len(s.split(TOKENIZER_SEPARATOR)) > 1
