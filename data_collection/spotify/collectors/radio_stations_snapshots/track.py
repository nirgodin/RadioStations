from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json

from consts.data_consts import NAME, ALBUM, POPULARITY, TRACK_NUMBER, DURATION_MS, EXPLICIT, RELEASE_DATE, TOTAL_TRACKS
from data_collection.spotify.collectors.radio_stations_snapshots.artist import Artist


@dataclass_json
@dataclass
class Track:
    name: str
    main_artist: Artist
    popularity: int
    track_number: int
    duration_ms: int
    explicit: bool
    main_album: str
    release_date: str
    album_tracks_number: int
    added_at: Optional[str] = None

    @classmethod
    def from_raw_track(cls, raw_track: dict, artist: Artist) -> "Track":
        return cls(
            name=raw_track.get(NAME, ''),
            main_artist=artist,
            popularity=raw_track.get(POPULARITY, ''),
            track_number=raw_track.get(TRACK_NUMBER, ''),
            duration_ms=raw_track.get(DURATION_MS, ''),
            explicit=raw_track.get(EXPLICIT, ''),
            main_album=raw_track.get(ALBUM, {}).get(NAME, ''),
            release_date=raw_track.get(ALBUM, {}).get(RELEASE_DATE, ''),
            album_tracks_number=raw_track.get(ALBUM, {}).get(TOTAL_TRACKS, ''),
        )
