import os
from typing import Generator

import pandas as pd
from pandas import DataFrame

SPOTIFY_DIR = 'data/spotify'
NAME = 'name'
ADDED_AT = 'added_at'
STATION = 'station'


class PreProcessor:
    def concatenate_data(self):
        dataframes = list(self._generate_raw_dataframes())
        concatenated_data = pd.concat(dataframes)
        return concatenated_data.drop_duplicates(subset=[NAME, ADDED_AT, STATION])

    @staticmethod
    def _generate_raw_dataframes() -> Generator[DataFrame, None, None]:
        for root, dirs, files in os.walk(SPOTIFY_DIR, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                yield pd.read_csv(file_path)


if __name__ == '__main__':
    pp = PreProcessor()
    data = pp.concatenate_data()
    data.to_csv('data/toy_data.csv', index=False)
