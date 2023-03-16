from collections import Counter
from functools import reduce
from typing import Optional, Generator

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import NAME, ARTIST_NAME, GENRES, GENRE, COUNT
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, GENRES_MAPPING_PATH


class GenreAnalyzer(IAnalyzer):
    def __init__(self, data_path: str = MERGED_DATA_PATH, output_path: Optional[str] = None):
        self._data_path = data_path
        self._output_path = output_path

    def analyze(self) -> None:
        data = pd.read_csv(self._data_path)
        non_duplicated_data = data.drop_duplicates(subset=[NAME, ARTIST_NAME])
        genres_count = self._get_genres_count(non_duplicated_data)

        if self._output_path is not None:
            genres_count.to_csv(self._output_path, index=False, encoding=UTF_8_ENCODING)

    def _get_genres_count(self, data: DataFrame) -> DataFrame:
        genres_counts = self._generate_tracks_genres_count(data)
        merged_count = reduce(lambda counter1, counter2: counter1 + counter2, genres_counts)
        formatted_merged_count = {genre: [count] for genre, count in merged_count.items()}
        merged_count_data = pd.DataFrame.from_dict(formatted_merged_count).transpose()

        return self._pre_process_genres_count(merged_count_data)

    @staticmethod
    def _generate_tracks_genres_count(data: DataFrame) -> Generator[Counter, None, None]:
        with tqdm(total=len(data)) as progress_bar:
            for i, row in data.iterrows():
                progress_bar.update(1)
                track_genres = eval(row[GENRES])

                if isinstance(track_genres, list):
                    yield Counter(track_genres)

    @staticmethod
    def _pre_process_genres_count(genres_count: DataFrame) -> DataFrame:
        genres_count.reset_index(level=0, inplace=True)
        genres_count.columns = [GENRE, COUNT]
        genres_count.sort_values(by=COUNT, ascending=False, inplace=True)

        return genres_count

    @property
    def name(self) -> str:
        return 'genre analyzer'


if __name__ == '__main__':
    analyzer = GenreAnalyzer(
        data_path=MERGED_DATA_PATH,
        output_path=GENRES_MAPPING_PATH
    )
    analyzer.analyze()
