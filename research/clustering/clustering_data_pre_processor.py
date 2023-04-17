from typing import List, Dict

import kneed
import numpy as np
import pandas as pd
from kneed import KneeLocator
from matplotlib import pyplot as plt
from pandas import DataFrame
from sklearn.impute import SimpleImputer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from tqdm import tqdm

from consts.aggregation_consts import FIRST, MEDIAN, MAX, MIN
from consts.clustering_consts import DROPPABLE_COLUMNS, CATEGORICAL_COLUMNS, GROUPBY_FIRST_COLUMNS, \
    GROUPBY_MIN_MAX_MEDIAN_COLUMNS, CATEGORICAL_COLUMNS_AGGREGATION_MAPPING
from consts.data_consts import SONG
from consts.path_consts import MERGED_DATA_PATH
from utils.general_utils import chain_dicts
from sklearn.cluster import DBSCAN, KMeans
from collections import Counter


class ClusteringDataPreProcessor:
    def pre_process(self, data: DataFrame) -> DataFrame:
        groubyed_data = self._groupby_data(data)
        scaled_data = MinMaxScaler().fit_transform(groubyed_data)
        # min_samples = int(2 * scaled_data.shape[1])
        # eps = self._find_optimal_epsilon(scaled_data, min_samples)
        # db = DBSCAN(eps=eps, min_samples=min_samples)
        # db.fit(scaled_data)
        k = self._find_optimal_epsilon(scaled_data, 1)
        km = KMeans(n_clusters=k, random_state=0)
        km.fit(scaled_data)
        # labels_count = Counter(db.labels_.tolist())
        print('b')

    def _find_optimal_epsilon(self, scaled_data: DataFrame, min_samples: int) -> int:
        sum_squared_dist = []
        with tqdm(total=50) as progress_bar:
            for k in range(1, 50):
                km = KMeans(n_clusters=k, random_state=0)
                km.fit(scaled_data)
                sum_squared_dist.append(km.inertia_)
                progress_bar.update(1)

        x = list(range(50))
        y = sum_squared_dist
        kn = KneeLocator(x, y, curve='convex', direction='decreasing')

        return kn.knee

    def _groupby_data(self, data: DataFrame) -> DataFrame:
        relevant_data = data.drop(DROPPABLE_COLUMNS, axis=1)
        relevant_data_with_dummies = pd.get_dummies(relevant_data, columns=CATEGORICAL_COLUMNS)
        non_song_columns = [col for col in relevant_data_with_dummies.columns if col != SONG]
        non_song_data = relevant_data_with_dummies[non_song_columns]
        imputed_data = pd.DataFrame(SimpleImputer(strategy=MEDIAN).fit_transform(non_song_data), columns=non_song_columns)
        imputed_data[SONG] = relevant_data_with_dummies[SONG]
        aggregation_mapping = self._build_group_by_aggregation_mapping(imputed_data.columns.tolist())

        return imputed_data.groupby(SONG).agg(aggregation_mapping)

    def _build_group_by_aggregation_mapping(self, columns: List[str]) -> Dict[str, List[str]]:
        first_mapping = {k: [FIRST] for k in GROUPBY_FIRST_COLUMNS}
        min_max_median_mapping = {k: [MIN, MAX, MEDIAN] for k in GROUPBY_MIN_MAX_MEDIAN_COLUMNS}
        categorical_columns_mapping = self._build_categorical_columns_aggregation_mapping(columns)
        mappings = [first_mapping, min_max_median_mapping, categorical_columns_mapping]

        return chain_dicts(mappings)

    @staticmethod
    def _build_categorical_columns_aggregation_mapping(columns: List[str]) -> Dict[str, List[str]]:
        aggregation_mapping = {}

        for prefix, agg_methods in CATEGORICAL_COLUMNS_AGGREGATION_MAPPING.items():
            prefix_columns = [col for col in columns if col.startswith(prefix)]

            for k in prefix_columns:
                aggregation_mapping[k] = agg_methods

        return aggregation_mapping

    @property
    def name(self) -> str:
        return 'clustering data pre processor'


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH)
    ClusteringDataPreProcessor().pre_process(data)
