from typing import Dict, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from consts.audio_features_consts import KEY
from consts.data_consts import ID, SPOTIFY_ID, GENIUS_ID, SONG
from consts.musixmatch_consts import MUSIXMATCH_ID, MUSIXMATCH_TRACK_ID
from consts.path_consts import SHAZAM_TRACKS_IDS_PATH, MUSIXMATCH_TRACK_IDS_PATH, GENIUS_TRACKS_IDS_OUTPUT_PATH, \
    TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH
from consts.shazam_consts import APPLE_MUSIC_ADAM_ID
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from utils.data_utils import read_merged_data
from utils.file_utils import read_json, to_csv

SHAZAM_ID_COLUMNS = [KEY, APPLE_MUSIC_ADAM_ID]


class TracksIDsMappingAnalyzer(IAnalyzer):
    def analyze(self) -> None:
        data = self._load_unique_tracks_ids()
        data_with_shazam_keys = self._merge_shazam_ids(data)
        data_with_musixmatch_keys = self._merge_musixmatch_ids(data_with_shazam_keys)
        data_with_genius_ids = self._merge_genius_ids(data_with_musixmatch_keys)
        data_with_genius_ids.drop(SONG, axis=1, inplace=True)

        to_csv(data=data_with_genius_ids, output_path=TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH)

    @staticmethod
    def _load_unique_tracks_ids() -> DataFrame:
        data = read_merged_data()
        data.drop_duplicates(subset=[ID], inplace=True)

        return data[[ID, SONG]]

    def _merge_shazam_ids(self, data: DataFrame) -> DataFrame:
        shazam_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        shazam_data.drop_duplicates(subset=[SPOTIFY_ID], inplace=True)
        shazam_data[SHAZAM_ID_COLUMNS] = shazam_data[SHAZAM_ID_COLUMNS].applymap(self._stringify_float)
        shazam_data.rename(columns={KEY: SHAZAM_KEY, SPOTIFY_ID: ID}, inplace=True)

        return data.merge(
            right=shazam_data[[ID, SHAZAM_KEY, APPLE_MUSIC_ADAM_ID]],
            on=ID,
            how="left"
        )

    def _merge_musixmatch_ids(self, data: DataFrame) -> DataFrame:
        musixmatch_data = read_json(MUSIXMATCH_TRACK_IDS_PATH)
        data[MUSIXMATCH_ID] = data[ID].apply(lambda x: self._extract_musixmatch_id(x, musixmatch_data))

        return data

    def _merge_genius_ids(self, data: DataFrame) -> DataFrame:
        genius_data = pd.read_csv(GENIUS_TRACKS_IDS_OUTPUT_PATH)
        genius_data.dropna(subset=[ID], inplace=True)
        genius_data[GENIUS_ID] = genius_data[ID].apply(self._stringify_float)

        return data.merge(
            right=genius_data[[SONG, GENIUS_ID]],
            how="left",
            on=SONG
        )

    def _extract_musixmatch_id(self, spotify_id: str, musixmatch_data: Dict[str, dict]) -> Union[float, str]:
        raw_id = musixmatch_data.get(spotify_id, {}).get(MUSIXMATCH_TRACK_ID, np.nan)
        return self._stringify_float(raw_id)

    @staticmethod
    def _stringify_float(x: float):
        return np.nan if pd.isna(x) else str(int(x))

    @property
    def name(self) -> str:
        return "tracks ids mapping analyzer"


if __name__ == '__main__':
    TracksIDsMappingAnalyzer().analyze()