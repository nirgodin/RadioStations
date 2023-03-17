import pandas as pd
from pandas import DataFrame

from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT
from consts.playlists_consts import STATIONS
from data_collection.spotify.station import Station
from utils.datetime_utils import get_current_datetime


class RadioStationsSnapshotsRunner:
    def run(self) -> None:
        data = self._get_radio_stations_snapshots()
        now = get_current_datetime()
        output_path = RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT.format(now)

        data.to_csv(output_path, encoding=UTF_8_ENCODING, index=False)

    @staticmethod
    def _get_radio_stations_snapshots() -> DataFrame:
        dfs = []

        for station_name, station_id in STATIONS.items():
            station = Station(name=station_name, id=station_id)
            dfs.append(station.to_dataframe())

        return pd.concat(dfs)
