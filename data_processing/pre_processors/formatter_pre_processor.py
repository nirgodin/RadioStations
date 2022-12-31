from typing import Dict, List

from pandas import DataFrame

from consts.data_consts import STATION, SONG, ARTIST_NAME, NAME
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
        relevant_stations_data.dropna(subset=[NAME, ARTIST_NAME], inplace=True)
        stations_names_mapping = self._map_raw_stations_to_formatted_stations(data)
        relevant_stations_data[STATION] = relevant_stations_data[STATION].map(stations_names_mapping)
        relevant_stations_data[SONG] = self._create_song_column(relevant_stations_data)

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
        titlized_tokens = [token.title() if token != 'fm' else 'FM' for token in split_station_name]

        return ' '.join(titlized_tokens)

    @staticmethod
    def _create_song_column(data: DataFrame) -> List[str]:
        songs = []

        for i, row in data.iterrows():
            artist_name = row[ARTIST_NAME]
            name = row[NAME]
            song_name = f'{artist_name} - {name}'
            songs.append(song_name)

        return songs

    @property
    def name(self) -> str:
        return 'formatter pre processor'
