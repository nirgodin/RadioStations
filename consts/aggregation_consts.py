from typing import Any

from pandas import Series
from scipy import stats

MIN = 'min'
MAX = 'max'
MEDIAN = 'median'
COUNT = 'count'
SUM = 'sum'
FIRST = 'first'
MEAN = 'mean'


def mode(series: Series) -> Any:
    return stats.mode(series)[0][0]
