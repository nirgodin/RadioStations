from typing import Optional
from datetime import datetime

from pandas import DataFrame

DEFAULT_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
DATETIME = 'datetime'
DAY = 'day'
MONTH = 'month'
YEAR = 'year'
DATE = 'date'
POSSIBLE_DATE_TYPES = [DAY, MONTH, YEAR, DATE]
POSSIBLE_DATE_TYPES_ASSERTION_FAIL_MESSAGE = f'date time must be one of the following `{", ".join(POSSIBLE_DATE_TYPES)}`'


class DatetimeManager:
    def add_date_string_column(self, data: DataFrame, date_type: str, output_colname: Optional[str] = None,
                               datetime_colname: Optional[str] = DATETIME) -> DataFrame:
        if output_colname is None:
            output_colname = date_type

        data[output_colname] = [self._from_datetime(datetime_object, date_type)
                                for datetime_object in data[datetime_colname]]

        return data

    def add_datetime_column(self, data: DataFrame, str_date_colname: str, output_colname: Optional[str] = DATETIME,
                            datetime_format: Optional[str] = DEFAULT_DATETIME_FORMAT) -> DataFrame:
        data[output_colname] = [self._to_datetime(datetime_string, datetime_format)
                                for datetime_string in data[str_date_colname]]

        return data

    @staticmethod
    def _from_datetime(datetime_object: datetime, date_type: str) -> str:
        assert date_type in POSSIBLE_DATE_TYPES, POSSIBLE_DATE_TYPES_ASSERTION_FAIL_MESSAGE

        if date_type == YEAR:
            return str(datetime_object.year)

        elif date_type == MONTH:
            return str(datetime_object.month)

        elif date_type == DAY:
            return str(datetime_object.day)

        else:
            date = datetime_object.date()
            return date.strftime("%d-%m-%Y")

    @staticmethod
    def _to_datetime(datetime_string: str, datetime_format: str) -> datetime:
        return datetime.strptime(datetime_string, datetime_format)
