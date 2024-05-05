from string import ascii_lowercase, ascii_uppercase
from typing import List, Generator

import pandas as pd
from genie_common.tools import SyncPoolExecutor
from pandas import Series, DataFrame

from utils.file_utils import to_csv


class EurovisionWritersAggregator:
    def __init__(self):
        self._pool_executor = SyncPoolExecutor()

    def aggregate(self):
        data = pd.read_csv(r'data/eurovision/tracks_languages.csv')
        records = self._pool_executor.run(
            iterable=[row for _, row in data.iterrows()],
            func=self._get_single_song_records,
            expected_type=DataFrame
        )
        data = pd.concat(records)

        to_csv(data, r"data/eurovision/songwriters.csv")

    def _get_single_song_records(self, row: Series) -> DataFrame:
        raw_writers = row["Songwriter(s)"]
        records = []

        for writer in self._split_songwriters(raw_writers):
            record = {
                "writer": writer,
                "country": row["Country"],
                "year": row["year"]
            }
            records.append(record)

        return pd.DataFrame.from_records(records)

    def _split_songwriters(self, raw_writers: str) -> Generator[str, None, None]:
        start_index = 0
        previous_char = "Initialized without lower string on purpose to make condition fail on first iteration"

        for i, current_char in enumerate(raw_writers):
            if previous_char in ascii_lowercase and current_char in ascii_uppercase:
                yield self._slice_writer_from_string(
                    string=raw_writers,
                    start_index=start_index,
                    end_index=i
                )
                start_index = i

            previous_char = current_char

        yield self._slice_writer_from_string(
            string=raw_writers,
            start_index=start_index,
            end_index=len(raw_writers)
        )

    @staticmethod
    def _slice_writer_from_string(string: str, start_index: int, end_index: int):
        letters = string[start_index:end_index]
        return "".join(letters)


if __name__ == '__main__':
    EurovisionWritersAggregator().aggregate()