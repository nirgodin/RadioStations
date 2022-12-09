from collections import Counter
from functools import reduce
from typing import Optional, Generator

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm


class GenreAnalyzer:
    def analyze(self, data_path: str, output_path: Optional[str] = None) -> DataFrame:
        data = pd.read_csv(data_path)
        non_duplicated_data = data.drop_duplicates(subset=['name', 'artist_name'])
        genres_count = self._get_genres_count(non_duplicated_data)

        if output_path is not None:
            genres_count.to_csv(output_path, index=False)

        return genres_count

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
                track_genres = eval(row['genres'])

                if isinstance(track_genres, list):
                    yield Counter(track_genres)

    @staticmethod
    def _pre_process_genres_count(genres_count: DataFrame) -> DataFrame:
        genres_count.reset_index(level=0, inplace=True)
        genres_count.columns = ['genre', 'count']
        genres_count.sort_values(by='count', ascending=False, inplace=True)

        return genres_count


if __name__ == '__main__':
    path = r'/data/merged_data.csv'
    output_path = r'/data/genres_mapping.csv'
    GenreAnalyzer().analyze(path, output_path)
