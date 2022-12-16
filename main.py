import pandas as pd

from consts.playlists_consts import STATIONS
from data_collection.station import Station
from data_collection.utils import get_current_datetime


def run():
    dfs = []

    for station_name, station_id in STATIONS.items():
        station = Station(name=station_name, id=station_id)
        dfs.append(station.to_dataframe())

    data = pd.concat(dfs)
    now = get_current_datetime()
    data.to_csv(rf'data/spotify/{now}.csv', encoding='utf-8-sig', index=False)


if __name__ == '__main__':
    run()
