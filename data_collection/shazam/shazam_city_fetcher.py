from typing import Dict, List

import pandas as pd
from shazamio import Shazam
from tqdm import tqdm

from consts.data_consts import ID, NAME
from consts.media_forest_consts import RANK
from consts.path_consts import SHAZAM_CHARTS_METADATA_PATH, SHAZAM_CITIES_PATH_FORMAT
from consts.shazam_consts import ISRAEL_COUNTRY_CODE, COUNTRIES, CITIES
from data_collection.shazam.shazam_utils import extract_tracks_data, to_csv
from utils.file_utils import read_json


class ShazamCitiesFetcher:
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam

    async def fetch_cities_top_tracks(self) -> None:
        israel_cities = self._get_israel_cities_ids()
        cities_top_tracks = await self._fetch_cities_tracks(israel_cities)
        data = self._extract_relevant_tracks_info(cities_top_tracks)

        to_csv(data=data, output_path_format=SHAZAM_CITIES_PATH_FORMAT)

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

    @staticmethod
    def _extract_relevant_tracks_info(cities_top_tracks: Dict[str, dict]):
        cities_data = []

        for city_name, tracks_data in cities_top_tracks.items():
            city_data = extract_tracks_data(tracks_data=tracks_data, location=city_name)
            city_data[RANK] = city_data.index + 1
            cities_data.append(city_data)

        return pd.concat(cities_data).reset_index(drop=True)
