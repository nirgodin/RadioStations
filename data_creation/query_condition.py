from dataclasses import dataclass
from typing import Any

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class QueryCondition:
    column: str
    operator: str
    value: Any

    def __post_init__(self):
        self.condition = f'{self.column} {self.operator} {self.value}'
