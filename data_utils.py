import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, POPULARITY
from consts.path_consts import MERGED_DATA_PATH


def groupby_artists_by_desc_popularity() -> DataFrame:
    data = pd.read_csv(MERGED_DATA_PATH)
    artists_popularity_data = data[[ARTIST_NAME, POPULARITY]]
    artists_mean_popularity = artists_popularity_data.groupby(by=ARTIST_NAME).mean()
    artists_mean_popularity.reset_index(level=0, inplace=True)
    artists_mean_popularity.sort_values(by=POPULARITY, ascending=False, inplace=True)

    return artists_mean_popularity


def map_df_columns(data: DataFrame, key_column: str, value_column: str) -> dict:
    return {
        key: value for key, value in zip(data[key_column], data[value_column])
    }
