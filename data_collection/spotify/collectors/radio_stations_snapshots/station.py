from dataclasses import dataclass
from typing import List

import pandas as pd
from dataclasses_json import dataclass_json
from pandas import DataFrame

from consts.data_consts import TRACKS
from data_collection.spotify.collectors.radio_stations_snapshots.playlist import Playlist
from data_collection.spotify.collectors.radio_stations_snapshots.track import Track
from utils.general_utils import recursively_flatten_nested_dict


@dataclass_json
@dataclass
class Station:
    playlist: Playlist
    tracks: List[Track]

    def to_dataframe(self) -> DataFrame:
        playlist = self.playlist.to_dict()
        playlist.pop(TRACKS)
        records = []

        for track in self.tracks:
            record = recursively_flatten_nested_dict(track.to_dict())
            record.update(playlist)
            records.append(record)

        return pd.DataFrame.from_records(records)
