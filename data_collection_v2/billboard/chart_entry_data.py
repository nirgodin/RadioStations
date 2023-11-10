from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from billboard import ChartEntry
from postgres_client.models.enum.billboard_chart import BillboardChart


@dataclass
class ChartEntryData:
    entry: ChartEntry
    date: datetime
    chart: BillboardChart  # TODO: Fix this import
    track: Optional[dict] = None

    def __post_init__(self):
        self.id = f"{self.entry.artist} - {self.entry.title}"
