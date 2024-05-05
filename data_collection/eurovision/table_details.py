from dataclasses import dataclass
from typing import Optional

from pandas import DataFrame


@dataclass
class TableDetails:
    data: DataFrame
    year: int
    stage: str
    voting_method: Optional[str] = None
