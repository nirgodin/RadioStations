from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from consts.data_consts import FOLLOWERS, TOTAL, SNAPSHOT_ID, TRACKS, ITEMS


@dataclass_json
@dataclass
class Playlist:
    station: str
    followers: int
    snapshot_id: str
    tracks: List[dict]

    @classmethod
    def from_spotify_response(cls, station_name: str, playlist: dict) -> "Playlist":
        return cls(
            station=station_name,
            followers=playlist.get(FOLLOWERS, {}).get(TOTAL, -1),
            snapshot_id=playlist.get(SNAPSHOT_ID, ''),
            tracks=playlist.get(TRACKS, {}).get(ITEMS, [])
        )
