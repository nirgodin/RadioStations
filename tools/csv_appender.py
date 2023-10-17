from typing import List, Optional

import pandas as pd
from pandas import DataFrame

from consts.miscellaneous_consts import UTF_8_ENCODING


class CSVAppender:
    @staticmethod
    def append(data: DataFrame, output_path: str, escapechar: Optional[str]) -> None:
        new_columns = data.columns.tolist()
        columns = CSVAppender._get_column_names(new_columns, output_path)
        missing_new_columns = [col for col in columns if col not in new_columns]

        if missing_new_columns:
            data[missing_new_columns] = ''

        ordered_data = data[columns]
        ordered_data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING, header=False, mode='a', escapechar=escapechar)

    @staticmethod
    def _get_column_names(new_columns: List[str], output_path: str) -> List[str]:
        existing_columns = CSVAppender._get_ordered_existing_columns_names(output_path)
        missing_columns = [col for col in new_columns if col not in existing_columns]

        if missing_columns:
            CSVAppender._add_existing_data_missing_columns(output_path, missing_columns)

        return existing_columns + missing_columns

    @staticmethod
    def _get_ordered_existing_columns_names(output_path: str) -> List[str]:
        return pd.read_csv(output_path, nrows=1).columns.tolist()

    @staticmethod
    def _add_existing_data_missing_columns(output_path: str, missing_columns: List[str]) -> None:
        data = pd.read_csv(output_path)
        data[missing_columns] = ''

        data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING)
