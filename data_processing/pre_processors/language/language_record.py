from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class LanguageRecord:
    id: str
    language: str
    score: float
    lyrics_source: str
