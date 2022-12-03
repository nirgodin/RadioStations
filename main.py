import re
from datetime import datetime

import pandas as pd

from data_collection.station import Station

STATIONS = {
    'glglz': '18cUFeM5Q75ViwevsMQM1j',
    'kan_88': '6EOBRaHfU63LNFKLJmeS3S',
    'eco_99': '07QpDB6YxM6flpbmdZTZNX',
    'kan_gimel': '0JFp62JAJSri8WbxEYdFCY',
    '103_fm': '2pBih0TFRCULBEOhKcFQ7Y',
    'spotify_top_50_weekly': '37i9dQZEVXbJ5J1TrbkAF9',
    'spotify_top_50_israel_daily': '37i9dQZEVXbJ6IpvItkve3',
    'spotify_top_50_global_daily': '37i9dQZEVXbMDoHDwVN2tF',
    'spotify_viral_50_israel': '37i9dQZEVXbNGlbFNNXxgC'
}


if __name__ == '__main__':
    dfs = []
    for station_name, station_id in STATIONS.items():
        station = Station(name=station_name, id=station_id)
        dfs.append(station.to_dataframe())

    data = pd.concat(dfs)

    now = str(datetime.now()).replace('.', '-')
    now = re.sub(r'[^\w\s]', '_', now)

    data.to_csv(rf'data/spotify/{now}.csv', encoding='utf-8-sig', index=False)
