import os
from typing import Generator, Optional, List

import pandas as pd
from pandas import DataFrame
from pandas.errors import EmptyDataError
from tqdm import tqdm

from consts.data_consts import NAME, ADDED_AT, STATION, SCRAPED_AT
from consts.miscellaneous_consts import CSV_FILE_SUFFIX


class DataMerger:
    @staticmethod
    def merge(dir_path: str,
              output_path: Optional[str] = None,
              drop_duplicates_on: List[str] = (NAME, ADDED_AT, STATION)) -> DataFrame:
        files_data = DataMerger._generate_files_data(dir_path)
        merged_data: DataFrame = pd.concat(files_data)
        non_duplicated_data = merged_data.drop_duplicates(subset=drop_duplicates_on)

        if output_path is not None:
            non_duplicated_data.to_csv(output_path, index=False)

        return non_duplicated_data

    @staticmethod
    def _generate_files_data(dir_path: str) -> Generator[DataFrame, None, None]:
        dir_files = os.listdir(dir_path)

        with tqdm(total=len(dir_files)) as progress_bar:
            for file_name in dir_files:
                progress_bar.update(1)

                if DataMerger._is_csv_file(file_name):
                    file_data = DataMerger._wrap_generate_single_file_data(dir_path, file_name)

                    if file_data is not None:
                        yield file_data

    @staticmethod
    def _wrap_generate_single_file_data(dir_path: str, file_name: str) -> Optional[DataFrame]:
        try:
            return DataMerger._generate_single_file_data(dir_path, file_name)
        except EmptyDataError:
            print(f'The following file is empty `{file_name}`. Skipping')

    @staticmethod
    def _generate_single_file_data(dir_path: str, file_name: str) -> DataFrame:
        file_path = os.path.join(dir_path, file_name)
        file_data = pd.read_csv(file_path)
        file_data[SCRAPED_AT] = file_name.replace(CSV_FILE_SUFFIX, '')

        return file_data

    @staticmethod
    def _is_csv_file(file_name: str) -> bool:
        return file_name.endswith(CSV_FILE_SUFFIX)
