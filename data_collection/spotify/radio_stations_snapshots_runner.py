import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from consts.path_consts import RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT
from consts.playlists_consts import STATIONS
from data_collection.spotify.data_classes.station import Station
from utils.datetime_utils import get_current_datetime
from utils.file_utils import to_csv


class RadioStationsSnapshotsRunner:
    def run(self) -> None:
        print('Starting to run radio stations snapshots runner')
        data = self._get_radio_stations_snapshots()
        now = get_current_datetime()
        output_path = RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT.format(now)

        to_csv(data=data, output_path=output_path)

    @staticmethod
    def _get_radio_stations_snapshots() -> DataFrame:
        dfs = []

        with tqdm(total=len(STATIONS)) as progress_bar:
            for station_name, station_id in STATIONS.items():
                station = Station(name=station_name, id=station_id)
                dfs.append(station.to_dataframe())
                progress_bar.update(1)

        return pd.concat(dfs)
