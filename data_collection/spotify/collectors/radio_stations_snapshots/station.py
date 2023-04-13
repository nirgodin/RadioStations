from dataclasses import dataclass
from typing import List, Dict

from dataclasses_json import dataclass_json

from consts.data_consts import FOLLOWERS, TOTAL, SNAPSHOT_ID, TRACKS, ITEMS


@dataclass_json
@dataclass
class Station:
    name: str
    id: str
    followers: int
    snapshot_id: str
    tracks: List[dict]

    @classmethod
    def from_playlist(cls, station_name: str, playlist: dict) -> "Station":
        return cls(
            name=station_name,
            id=playlist.get(FOLLOWERS, {}).get(TOTAL, -1),
            followers=playlist.get(FOLLOWERS, {}).get(TOTAL, -1),
            snapshot_id=playlist.get(SNAPSHOT_ID, ''),
            tracks=playlist.get(TRACKS, {}).get(ITEMS, [])
        )
