import pandas as pd

from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.playlists_consts import STATIONS
from data_collection.spotify.station import Station
from utils import get_current_datetime


def run():
    dfs = []

    for station_name, station_id in STATIONS.items():
        station = Station(name=station_name, id=station_id)
        dfs.append(station.to_dataframe())

    data = pd.concat(dfs)
    now = get_current_datetime()
    data.to_csv(rf'data/spotify/{now}.csv', encoding=UTF_8_ENCODING, index=False)


if __name__ == '__main__':
    run()
