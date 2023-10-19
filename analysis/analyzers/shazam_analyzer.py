from typing import Dict

import numpy as np
import pandas as pd
from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from consts.audio_features_consts import KEY
from consts.data_consts import SCRAPED_AT, NAME, ARTIST_NAME, SONG
from consts.media_forest_consts import RANK
from consts.path_consts import SHAZAM_ISRAEL_DIR_PATH, SHAZAM_ISRAEL_MERGED_DATA, SHAZAM_WORLD_DIR_PATH, \
    SHAZAM_CITIES_DIR_PATH, SHAZAM_APPLE_TRACKS_IDS_MAPPING_OUTPUT_PATH
from consts.shazam_consts import SHAZAM_RANK, TITLE, SUBTITLE, ISRAEL, WORLD, CITIES, APPLE_MUSIC_TRACK_ID
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from utils.file_utils import to_csv

SHAZAM_COLUMNS_MAPPING = {
    KEY: SHAZAM_KEY,
    RANK: SHAZAM_RANK,
    TITLE: NAME,
    SUBTITLE: ARTIST_NAME
}


class ShazamAnalyzer(IAnalyzer):
    def __init__(self):
        self._data_merger = DataMerger(drop_duplicates_on=[KEY, SCRAPED_AT])

    def analyze(self) -> None:
        merged_data_mapping = self._get_shazam_merged_data_mapping()
        self._output_formatted_israeli_data(merged_data_mapping)
        self._output_shazam_key_to_apple_id_mapping(merged_data_mapping)

    def _get_shazam_merged_data_mapping(self) -> Dict[str, DataFrame]:
        data_mapping = {}

        for location, dir_path in self._shazam_location_to_data_dir_mapping.items():
            print(f'Merging shazam data for location `{location}`')
            data_mapping[location] = self._data_merger.merge(dir_path)

        return data_mapping

    def _output_formatted_israeli_data(self, merged_data_mapping: Dict[str, DataFrame]) -> None:
        print('Saving shazam israeli formatted data')
        data = merged_data_mapping[ISRAEL].copy()
        data.rename(columns=SHAZAM_COLUMNS_MAPPING, inplace=True)
        data[SONG] = data[[ARTIST_NAME, NAME]].apply(lambda x: self._to_song(*x), axis=1)

        to_csv(data, SHAZAM_ISRAEL_MERGED_DATA)

    @staticmethod
    def _to_song(artist_name: str, name: str) -> str:
        return f'{artist_name} - {name}'

    @staticmethod
    def _output_shazam_key_to_apple_id_mapping(merged_data_mapping: Dict[str, DataFrame]) -> None:
        print('Starting to map shazam keys to apple ids')
        non_duplicated_data = [df.drop_duplicates(subset=[KEY]) for df in merged_data_mapping.values()]
        data = pd.concat(non_duplicated_data)
        data.rename(columns=SHAZAM_COLUMNS_MAPPING, inplace=True)
        ids_mapping = data[[SHAZAM_KEY, APPLE_MUSIC_TRACK_ID]]
        ids_mapping[APPLE_MUSIC_TRACK_ID] = ids_mapping[APPLE_MUSIC_TRACK_ID].apply(lambda x: np.nan if pd.isna(x) else str(int(x)))

        to_csv(data=ids_mapping, output_path=SHAZAM_APPLE_TRACKS_IDS_MAPPING_OUTPUT_PATH)

    @property
    def _shazam_location_to_data_dir_mapping(self) -> Dict[str, str]:
        return {
            ISRAEL: SHAZAM_ISRAEL_DIR_PATH,
            WORLD: SHAZAM_WORLD_DIR_PATH,
            CITIES: SHAZAM_CITIES_DIR_PATH
        }

    @property
    def name(self) -> str:
        return "Shazam analyzer"


if __name__ == '__main__':
    ShazamAnalyzer().analyze()
