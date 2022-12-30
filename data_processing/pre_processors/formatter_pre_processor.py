from typing import Dict

from pandas import DataFrame

from consts.data_consts import STATION
from consts.playlists_consts import SPOTIFY_VIRAL_50_ISRAEL, SPOTIFY_TOP_50_GLOBAL_DAILY, SPOTIFY_TOP_50_ISRAEL_DAILY, \
    SPOTIFY_TOP_50_WEEKLY
from data_processing.pre_processors.pre_processor_interface import IPreProcessor

IRRELEVANT_STATIONS = [
    SPOTIFY_TOP_50_WEEKLY,
    SPOTIFY_TOP_50_ISRAEL_DAILY,
    SPOTIFY_TOP_50_GLOBAL_DAILY,
    SPOTIFY_VIRAL_50_ISRAEL
]


class FormatterPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        relevant_stations_data = self._drop_irrelevant_stations(data)
        stations_names_mapping = self._map_raw_stations_to_formatted_stations(data)
        relevant_stations_data[STATION] = relevant_stations_data[STATION].map(stations_names_mapping)

        return relevant_stations_data

    @staticmethod
    def _drop_irrelevant_stations(data: DataFrame) -> DataFrame:
        return data[~data[STATION].isin(IRRELEVANT_STATIONS)]

    def _map_raw_stations_to_formatted_stations(self, data: DataFrame) -> Dict[str, str]:
        unique_stations = data[STATION].unique().tolist()
        return {station: self._format_station_name(station) for station in unique_stations}

    @staticmethod
    def _format_station_name(station_name: str) -> str:
        split_station_name = station_name.split('_')
        joined_station_name = ' '.join(split_station_name)

        return joined_station_name.title()

    @property
    def name(self) -> str:
        return 'formatter pre processor'
