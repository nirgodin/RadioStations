from datetime import datetime, timedelta

import pytz

DATETIME_FORMAT = '%Y_%m_%d %H_%M_%S_%f'
DAYS_IN_YEAR = 365.25


def get_current_datetime() -> str:
    return datetime.now().strftime(DATETIME_FORMAT)


def now_israel_timezone() -> datetime:
    now_utc = datetime.now(tz=pytz.utc)
    return now_utc + timedelta(hours=2)


def convert_timedelta_to_years(delta: timedelta) -> int:
    years = delta.days / DAYS_IN_YEAR
    return round(years)
