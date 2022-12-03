import re
from datetime import datetime

from shazammer import Shazammer
from utils import to_json


SHAZAM_CHARTS = {
    'beersheba': 'ip-city-chart-295530',
    'haifa': 'ip-city-chart-294801',
    'jerusalem': 'ip-city-chart-281184',
    'tel_aviv': 'ip-city-chart-293397'
}


if __name__ == '__main__':
    shazammer = Shazammer()
    shazam_tracks = shazammer.get_multiple_charts_tracks(charts_ids=list(SHAZAM_CHARTS.values()), number_of_tracks=80)

    now = str(datetime.now()).replace('.', '-')
    now = re.sub(r'[^\w\s]', '_', now)
    to_json(shazam_tracks, path=rf'data/shazam/{now}.json')
