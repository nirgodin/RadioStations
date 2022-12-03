import os
from functools import reduce
from typing import Generator, Optional

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

BASE_DIR = r'data/spotify'


class DataMerger:
    @staticmethod
    def merge(dir_path: str = BASE_DIR, output_path: Optional[str] = None) -> DataFrame:
        files_data = DataMerger._generate_files_data(dir_path)
        merged_data: DataFrame = pd.concat(files_data)  # reduce(lambda df1, df2: pd.concat([df1, df2]), files_data)
        non_duplicated_data = merged_data.drop_duplicates(subset=['name', 'added_at'])

        if output_path is not None:
            non_duplicated_data.to_csv(output_path, index=False)

        return non_duplicated_data

    @staticmethod
    def _generate_files_data(dir_path: str) -> Generator[DataFrame, None, None]:
        dir_files = os.listdir(dir_path)

        with tqdm(total=len(dir_files)) as progress_bar:
            for file_name in dir_files:
                progress_bar.update(1)

                yield DataMerger._generate_single_file_data(dir_path, file_name)

    @staticmethod
    def _generate_single_file_data(dir_path: str, file_name: str) -> DataFrame:
        file_path = os.path.join(dir_path, file_name)
        file_data = pd.read_csv(file_path)
        file_data['scraped_at'] = file_name

        return file_data
