from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from billboard import ChartEntry


@dataclass
class ChartEntryData:
    entry: ChartEntry
    date: datetime
    track: Optional[dict] = None
