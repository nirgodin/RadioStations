from typing import Any, Dict, List

import pandas as pd

from data_collection.spotify import get_spotipy
from data_collection.track import Track
from dacite import from_dict

FOLLOWERS = "followers"
TOTAL = 'total'
SNAPSHOT_ID = 'snapshot_id'
TRACKS = 'tracks'
ITEMS = 'items'


class Station:
    def __init__(self, name: str, id: str):
        self.sp = get_spotipy()
        self.name = name
        self.id = id

    @property
    def playlist(self) -> Dict[str, Any]:
        return self.sp.playlist(playlist_id=self.id)

    @property
    def followers(self) -> int:
        return self.playlist.get(FOLLOWERS, {}).get(TOTAL, -1)

    @property
    def snapshot_id(self) -> str:
        return self.playlist.get(SNAPSHOT_ID, '')

    @property
    def tracks(self) -> List[Track]:
        raw_tracks = self.playlist.get(TRACKS, {}).get(ITEMS, [])
        tracks = []

        for raw_track in raw_tracks:
            track = from_dict(data_class=Track, data=raw_track['track'])
            track.added_at = raw_track['added_at']
            tracks.append(track)

        return tracks

    def to_dict(self):
        d = self.__dict__
        d.update(
            {
                'followers': self.followers,
                'snapshot_id': self.snapshot_id,
                'tracks': [track.to_dict() for track in self.tracks]
            }
        )
        return d

    def to_dataframe(self):
        tracks_data = self._get_tracks_df()
        tracks_data['station'] = self.name
        tracks_data['followers'] = self.followers
        tracks_data['snapshot_id'] = self.snapshot_id

        return tracks_data

    def _get_tracks_df(self):
        tracks_dataframes = [track.to_dataframe() for track in self.tracks]
        return pd.concat(tracks_dataframes)
