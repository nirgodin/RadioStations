from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PlayCountDifferenceAnalyzerConfig:
    separation_date: datetime
    count_column: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    use_percentage: bool = True
    output_path: Optional[str] = None
