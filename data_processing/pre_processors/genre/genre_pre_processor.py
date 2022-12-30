from collections import Counter
from typing import Dict, List

from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import MAIN_GENRE, GENRES
from data_processing.pre_processors.genre.main_genre_mapper import MainGenreMapper, OTHER
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class GenrePreProcessor(IPreProcessor):
    def __init__(self):
        self._main_genre_mapper = MainGenreMapper()

    def pre_process(self, data: DataFrame) -> DataFrame:
        tracks_raw_genres = data[GENRES].unique().tolist()
        main_genres_mapping = self._get_main_genres_mapping(tracks_raw_genres)
        data[MAIN_GENRE] = data[GENRES].map(main_genres_mapping)

        return data

    def _get_main_genres_mapping(self, raw_genres: List[str]) -> Dict[str, str]:
        main_genres = {}

        with tqdm(total=len(raw_genres)) as progress_bar:
            for track_genres in raw_genres:
                main_genre = self._get_single_track_main_genre(track_genres)
                main_genres[track_genres] = main_genre
                progress_bar.update(1)

        return main_genres

    def _get_single_track_main_genre(self, raw_genres: str) -> str:
        main_genres = self._map_raw_genres_to_main_genres(raw_genres)

        if not main_genres:
            return NA

        non_other_main_genres = [genre for genre in main_genres if genre != OTHER]

        if not non_other_main_genres:
            return OTHER

        return self._extract_most_common_main_genre(non_other_main_genres)

    def _map_raw_genres_to_main_genres(self, raw_genres: str) -> List[str]:
        genres = self._to_list(raw_genres)
        mapped_genres = []

        for genre in genres:
            mapped_genre = self._main_genre_mapper.map(genre)
            mapped_genres.append(mapped_genre)

        return mapped_genres

    @staticmethod
    def _to_list(raw_genres: str) -> List[str]:
        return eval(raw_genres)

    @staticmethod
    def _extract_most_common_main_genre(main_genres: List[str]) -> str:
        main_genres_count = Counter(main_genres)
        most_common_main_genre = main_genres_count.most_common(1)
        genre_name, genre_count = most_common_main_genre[0]

        return genre_name

    @property
    def name(self) -> str:
        return 'genre pre processor'
