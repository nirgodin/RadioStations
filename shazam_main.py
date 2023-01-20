from data_collection.shazam.shazammer import Shazammer
from utils import to_json, get_current_datetime


SHAZAM_CHARTS = {
    'beersheba': 'ip-city-chart-295530',
    'haifa': 'ip-city-chart-294801',
    'jerusalem': 'ip-city-chart-281184',
    'tel_aviv': 'ip-city-chart-293397'
}


if __name__ == '__main__':
    shazammer = Shazammer()
    shazam_tracks = shazammer.get_multiple_charts_tracks(charts_ids=list(SHAZAM_CHARTS.values()), number_of_tracks=80)

    now = get_current_datetime()
    to_json(shazam_tracks, path=rf'data/shazam/{now}.json')
