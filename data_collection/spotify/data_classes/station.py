from typing import Any, Dict, List

import pandas as pd

from consts.data_consts import ADDED_AT, FOLLOWERS, TOTAL, SNAPSHOT_ID, TRACKS, ITEMS, STATION, TRACK
from utils.spotify_utils import get_spotipy
from data_collection.spotify.data_classes.track import Track
from dacite import from_dict


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
            track = from_dict(data_class=Track, data=raw_track[TRACK])
            track.added_at = raw_track[ADDED_AT]
            tracks.append(track)

        return tracks

    def to_dict(self):
        d = self.__dict__
        d.update(
            {
                FOLLOWERS: self.followers,
                SNAPSHOT_ID: self.snapshot_id,
                TRACKS: [track.to_dict() for track in self.tracks]
            }
        )
        return d

    def to_dataframe(self):
        tracks_data = self._get_tracks_df()
        tracks_data[STATION] = self.name
        tracks_data[FOLLOWERS] = self.followers
        tracks_data[SNAPSHOT_ID] = self.snapshot_id

        return tracks_data

    def _get_tracks_df(self):
        tracks_dataframes = [track.to_dataframe() for track in self.tracks]
        return pd.concat(tracks_dataframes)
