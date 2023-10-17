from dataclasses import dataclass
from typing import Callable


@dataclass
class ExtractionInstructions:
    source_column: str
    result_column: str
    extraction_method: Callable
