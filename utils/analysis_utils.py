from pandas import DataFrame

from consts.data_consts import NAME, COUNT


def aggregate_play_count(data: DataFrame, column: str) -> DataFrame:
    raw_count_data = data.groupby(by=column).count().reset_index(level=0)
    count_data = raw_count_data[[column, NAME]]
    count_data.columns = [column, COUNT]

    return count_data
