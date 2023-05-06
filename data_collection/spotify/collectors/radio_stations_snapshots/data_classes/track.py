from dataclasses import dataclass

from dataclasses_json import dataclass_json

from consts.data_consts import NAME, ALBUM, POPULARITY, TRACK_NUMBER, DURATION_MS, EXPLICIT, RELEASE_DATE, TOTAL_TRACKS, \
    TRACK, ADDED_AT
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.artist import Artist


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
    added_at: str

    @classmethod
    def from_raw_track(cls, track: dict, raw_track: dict, artist: Artist) -> "Track":
        album = track.get(ALBUM, {})

        return cls(
            name=track.get(NAME, ''),
            main_artist=artist,
            popularity=track.get(POPULARITY, ''),
            track_number=track.get(TRACK_NUMBER, ''),
            duration_ms=track.get(DURATION_MS, ''),
            explicit=track.get(EXPLICIT, ''),
            main_album=album.get(NAME, ''),
            release_date=album.get(RELEASE_DATE, ''),
            album_tracks_number=album.get(TOTAL_TRACKS, ''),
            added_at=raw_track.get(ADDED_AT, '')
        )
