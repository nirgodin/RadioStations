import asyncio
from typing import Dict, List

import pandas as pd
from pandas import DataFrame
from shazamio import Shazam
from tqdm import tqdm

from consts.data_consts import ID, NAME, TRACKS
from consts.path_consts import SHAZAM_CHARTS_METADATA_PATH, SHAZAM_CITIES_PATH_FORMAT
from consts.shazam_consts import CITY, ISRAEL_COUNTRY_CODE, COUNTRIES, CITIES
from data_collection.shazam.shazam_track_response_extractor import ShazamTrackResponseExtractor
from utils import read_json, get_current_datetime


class ShazamCitiesFetcher:
    def __init__(self,
                 shazam: Shazam = Shazam(),
                 track_response_extractor: ShazamTrackResponseExtractor = ShazamTrackResponseExtractor()):
        self._shazam = shazam
        self._track_response_extractor = track_response_extractor

    async def fetch_cities_top_tracks(self):
        israel_cities = self._get_israel_cities_ids()
        cities_top_tracks = await self._fetch_cities_tracks(israel_cities)
        data = self._extract_relevant_tracks_info(cities_top_tracks)

        data.to_csv(self._build_output_path(), index=False, encoding='utf-8-sig')

    def _get_israel_cities_ids(self) -> List[str]:
        charts_metadata = read_json(SHAZAM_CHARTS_METADATA_PATH)
        countries_metadata = charts_metadata[COUNTRIES]
        israel_metadata = self._extract_israel_metadata(countries_metadata)
        israel_cities_metadata = israel_metadata[CITIES]

        return [city[NAME] for city in israel_cities_metadata]

    @staticmethod
    def _extract_israel_metadata(countries_metadata: List[Dict[str, str]]) -> Dict[str, str]:
        for country in countries_metadata:
            if country[ID] == ISRAEL_COUNTRY_CODE:
                return country

    async def _fetch_cities_tracks(self, israel_cities: List[str]) -> Dict[str, dict]:
        cities_top_tracks = {}
        print(f'Starting to fetch shazam data for the following cities {israel_cities}')

        with tqdm(total=len(israel_cities)) as progress_bar:
            for city_name in israel_cities:
                city_top_tracks = await self._shazam.top_city_tracks(
                    country_code=ISRAEL_COUNTRY_CODE,
                    city_name=city_name,
                    limit=200
                )
                cities_top_tracks[city_name] = city_top_tracks
                progress_bar.update(1)

        return cities_top_tracks

    def _extract_relevant_tracks_info(self, cities_top_tracks: Dict[str, dict]):
        cities_data = []

        for city_name, tracks_data in cities_top_tracks.items():
            city_data = self._extract_single_city_data(city_name, tracks_data)
            cities_data.append(city_data)

        return pd.concat(cities_data).reset_index(drop=True)

    def _extract_single_city_data(self, city_name: str, tracks_data: dict) -> DataFrame:
        city_tracks: List[dict] = tracks_data[TRACKS]
        city_data = self._track_response_extractor.extract_multiple_tracks_info(city_tracks)
        city_data[CITY] = city_name

        return city_data

    @staticmethod
    def _build_output_path() -> str:
        now = get_current_datetime()
        return SHAZAM_CITIES_PATH_FORMAT.format(now)


if __name__ == '__main__':
    cities_fetcher = ShazamCitiesFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cities_fetcher.fetch_cities_top_tracks())
