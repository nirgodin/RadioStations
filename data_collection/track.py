from dataclasses import dataclass, asdict

import pandas as pd

from data_collection.artist import Artist

NAME = 'name'
ID = 'id'
RELEASE_DATE = 'release_date'
TOTAL_TRACKS = 'total_tracks'


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
        d.pop('album')
        d.pop('artists')
        d.update(self.main_artist.to_dict())
        d.update(
            {
                'main_album': self.main_album,
                'release_date': self.release_date,
                'album_tracks_number': self.album_tracks_number
            }
        )

        return d

    def to_dataframe(self):
        return pd.DataFrame.from_dict(self.to_dict(), orient='index').transpose()
