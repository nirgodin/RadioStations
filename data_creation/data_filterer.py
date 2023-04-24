import random
from typing import Dict, List

import pandas as pd
from pandas import DataFrame

from data_creation.query_condition import QueryCondition


class DataFilterer:
    def __init__(self, candidates_pool_size: int = 5000, max_playlist_size: int = 100):
        self._candidates_pool_size = candidates_pool_size
        self._max_playlist_size = max_playlist_size

    def filter(self, query_conditions: List[QueryCondition]) -> DataFrame:
        data = self._generate_candidates()
        query = self._build_query(query_conditions)
        filtered_data = data.query(query)

        return filtered_data.iloc[:self._max_playlist_size]

    def _generate_candidates(self) -> DataFrame:
        data = pd.read_csv(r'data/groubyed_songs.csv')
        candidates_indexes = [random.randint(0, len(data)) for i in range(self._candidates_pool_size)]
        candidates = data.iloc[candidates_indexes]
        candidates.reset_index(drop=True, inplace=True)

        return candidates

    @staticmethod
    def _build_query(query_conditions: List[QueryCondition]) -> str:
        conditions = [query_condition.condition for query_condition in query_conditions]
        return ' and '.join(conditions)


if __name__ == '__main__':
    query_conditions = [
        QueryCondition(
            column='duration_ms',
            operator='<',
            value=240000
        ),
        QueryCondition(
            column='energy',
            operator='<',
            value=0.8
        ),
        QueryCondition(
            column='energy',
            operator='<',
            value=0.8
        ),
        QueryCondition(
            column='energy',
            operator='>',
            value=0.4
        ),
        QueryCondition(
            column='explicit',
            operator='==',
            value=True
        ),
        QueryCondition(
            column='tempo',
            operator='>',
            value=100
        ),
        QueryCondition(
            column='median_popularity',
            operator='<',
            value=80
        ),
        QueryCondition(
            column='median_popularity',
            operator='>',
            value=40
        )
    ]
    DataFilterer().filter(query_conditions)
