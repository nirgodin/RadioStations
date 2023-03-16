import pandas as pd

from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT
from consts.playlists_consts import STATIONS
from data_collection.spotify.station import Station
from utils.general_utils import get_current_datetime


def run():
    dfs = []

    for station_name, station_id in STATIONS.items():
        station = Station(name=station_name, id=station_id)
        dfs.append(station.to_dataframe())

    data = pd.concat(dfs)
    now = get_current_datetime()
    output_path = RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT.format(now)

    data.to_csv(output_path, encoding=UTF_8_ENCODING, index=False)


if __name__ == '__main__':
    run()
