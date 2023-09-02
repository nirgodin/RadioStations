from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, NAME, COUNT


def get_artists_play_count(data: DataFrame) -> DataFrame:
    raw_count_data = data.groupby(by=ARTIST_NAME).count().reset_index(level=0)
    count_data = raw_count_data[[ARTIST_NAME, NAME]]
    count_data.columns = [ARTIST_NAME, COUNT]

    return count_data
