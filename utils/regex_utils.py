import re
from typing import List

import numpy as np

from consts.miscellaneous_consts import YEAR_REGEX


def search_between_two_characters(start_char: str, end_char: str, text: str) -> List[str]:
    return re.findall(f"{start_char}(.*?){end_char}", text)


def extract_year(date: str) -> int:
    match = YEAR_REGEX.match(date)

    if match is not None:
        return int(match.group(1))

    return np.nan