from dataclasses import dataclass, asdict

import pandas as pd

from consts.data_consts import ALBUM, MAIN_ALBUM, ARTISTS, RELEASE_DATE, NAME, ID, TOTAL_TRACKS, ALBUM_TRACKS_NUMBER
from data_collection.artist import Artist


@dataclass
class Track:
    name: str
    album: dict
    artists: list
    popularity: int
    track_number: int
    duration_ms: int
    explicit: bool
    added_at: str = None

    @property
    def main_artist(self) -> Artist:
        return Artist.create_from_id(id=self._get_artist_id())

    def _get_artist_id(self) -> str:
        return self.artists[0][ID]

    @property
    def main_album(self) -> str:
        return self.album.get(NAME, '')

    @property
    def release_date(self) -> str:
        return self.album.get(RELEASE_DATE, '')

    @property
    def album_tracks_number(self) -> int:
        return self.album.get(TOTAL_TRACKS, '')

    def to_dict(self):
        d = asdict(self)
        d.pop(ALBUM)
        d.pop(ARTISTS)
        d.update(self.main_artist.to_dict())
        d.update(
            {
                MAIN_ALBUM: self.main_album,
                RELEASE_DATE: self.release_date,
                ALBUM_TRACKS_NUMBER: self.album_tracks_number
            }
        )

        return d

    def to_dataframe(self):
        return pd.DataFrame.from_dict(self.to_dict(), orient='index').transpose()
