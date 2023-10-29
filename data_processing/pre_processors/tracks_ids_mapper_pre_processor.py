from typing import Dict, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from consts.audio_features_consts import KEY
from consts.data_consts import ID, SPOTIFY_ID, GENIUS_ID, SONG
from consts.musixmatch_consts import MUSIXMATCH_ID, MUSIXMATCH_TRACK_ID
from consts.path_consts import SHAZAM_TRACKS_IDS_PATH, MUSIXMATCH_TRACK_IDS_PATH, GENIUS_TRACKS_IDS_OUTPUT_PATH, \
    SHAZAM_APPLE_TRACKS_IDS_MAPPING_OUTPUT_PATH
from consts.shazam_consts import APPLE_MUSIC_ADAM_ID, APPLE_MUSIC_TRACK_ID, APPLE_MUSIC_ID
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.file_utils import read_json
from utils.general_utils import stringify_float

SHAZAM_ID_COLUMNS = [KEY, APPLE_MUSIC_ADAM_ID]


class TracksIDSMapperPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        tracks_ids_mapping = self._map_tracks_ids(data)
        return data.merge(
            right=tracks_ids_mapping,
            on=ID,
            how="left"
        )

    def _map_tracks_ids(self, data: DataFrame) -> DataFrame:
        tracks_ids_data = data.copy().drop_duplicates(subset=[ID])
        data_with_shazam_keys = self._merge_shazam_ids(tracks_ids_data[[ID, SONG]])
        data_with_musixmatch_keys = self._merge_musixmatch_ids(data_with_shazam_keys)
        data_with_genius_ids = self._merge_genius_ids(data_with_musixmatch_keys)

        return data_with_genius_ids.drop(SONG, axis=1)

    def _merge_shazam_ids(self, data: DataFrame) -> DataFrame:
        data.drop(SHAZAM_KEY, axis=1, inplace=True)
        shazam_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        shazam_data.drop_duplicates(subset=[SPOTIFY_ID], inplace=True)
        shazam_data[SHAZAM_ID_COLUMNS] = shazam_data[SHAZAM_ID_COLUMNS].applymap(stringify_float)
        shazam_data.rename(columns={KEY: SHAZAM_KEY, SPOTIFY_ID: ID}, inplace=True)
        shazam_data_with_apple_ids = self._merge_apple_ids(shazam_data)

        return data.merge(
            right=shazam_data_with_apple_ids[[ID, SHAZAM_KEY, APPLE_MUSIC_ADAM_ID, APPLE_MUSIC_ID]],
            on=ID,
            how="left"
        )

    @staticmethod
    def _merge_apple_ids(data: DataFrame) -> DataFrame:
        data.dropna(subset=[SHAZAM_KEY], inplace=True)
        data[SHAZAM_KEY] = data[SHAZAM_KEY].astype(str)
        shazam_apple_mapping = pd.read_csv(SHAZAM_APPLE_TRACKS_IDS_MAPPING_OUTPUT_PATH)
        shazam_apple_mapping.rename(columns={APPLE_MUSIC_TRACK_ID: APPLE_MUSIC_ID}, inplace=True)
        shazam_apple_mapping[SHAZAM_KEY] = shazam_apple_mapping[SHAZAM_KEY].astype(str)

        return data.merge(
            right=shazam_apple_mapping,
            how='left',
            on=SHAZAM_KEY
        )

    def _merge_musixmatch_ids(self, data: DataFrame) -> DataFrame:
        musixmatch_data = read_json(MUSIXMATCH_TRACK_IDS_PATH)
        data[MUSIXMATCH_ID] = data[ID].apply(lambda x: self._extract_musixmatch_id(x, musixmatch_data))

        return data

    @staticmethod
    def _merge_genius_ids(data: DataFrame) -> DataFrame:
        genius_data = pd.read_csv(GENIUS_TRACKS_IDS_OUTPUT_PATH)
        genius_data.dropna(subset=[ID], inplace=True)
        genius_data[GENIUS_ID] = genius_data[ID].apply(stringify_float)

        return data.merge(
            right=genius_data[[SONG, GENIUS_ID]],
            how="left",
            on=SONG
        )

    @staticmethod
    def _extract_musixmatch_id(spotify_id: str, musixmatch_data: Dict[str, dict]) -> Union[float, str]:
        raw_id = musixmatch_data.get(spotify_id, {}).get(MUSIXMATCH_TRACK_ID, np.nan)
        return stringify_float(raw_id)

    @property
    def name(self) -> str:
        return "tracks ids mapper pre processor"
