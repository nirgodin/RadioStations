from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from consts.audio_features_consts import KEY
from consts.data_consts import SCRAPED_AT, ADDED_AT, DATE_ADDED
from consts.datetime_consts import DATETIME_FORMAT, DATE_FORMAT, SPOTIFY_DATETIME_FORMAT
from consts.path_consts import SHAZAM_ISRAEL_MERGED_DATA, SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH
from consts.shazam_consts import SHAZAM_RANK, IS_IN_SHAZAM_200
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class ShazamPreProcessor(IPreProcessor):
    def __init__(self):
        self._data_merger = DataMerger(drop_duplicates_on=[KEY, SCRAPED_AT])

    def pre_process(self, data: DataFrame) -> DataFrame:
        shazam_200_merged_data = self._merge_shazam_200_data(data)
        return self._merge_shazam_tracks_about_data(shazam_200_merged_data)

    def _merge_shazam_200_data(self, data: DataFrame) -> DataFrame:
        data[DATE_ADDED] = data[ADDED_AT].apply(lambda x: self._to_date(x, SPOTIFY_DATETIME_FORMAT))
        shazam_data = self._load_shazam_data()
        merged_data = data.merge(
            how='left',
            on=[SHAZAM_KEY, DATE_ADDED],
            right=shazam_data
        )
        merged_data[IS_IN_SHAZAM_200] = merged_data[SHAZAM_RANK].apply(lambda x: not pd.isna(x))

        return merged_data.drop(DATE_ADDED, axis=1)

    def _load_shazam_data(self) -> DataFrame:
        shazam_data = pd.read_csv(SHAZAM_ISRAEL_MERGED_DATA)
        shazam_data[DATE_ADDED] = shazam_data[SCRAPED_AT].apply(lambda x: self._to_date(x, DATETIME_FORMAT))

        return shazam_data[[SHAZAM_KEY, SHAZAM_RANK, DATE_ADDED]]

    @staticmethod
    def _to_date(date: Union[str, float], datetime_format: str) -> Union[str, float]:
        if pd.isna(date):
            return np.nan

        date_time = datetime.strptime(date, datetime_format)
        return date_time.strftime(DATE_FORMAT)

    @staticmethod
    def _merge_shazam_tracks_about_data(data: DataFrame) -> DataFrame:
        shazam_tracks_about_data = pd.read_csv(SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH)
        return data.merge(
            right=shazam_tracks_about_data,
            on=SHAZAM_KEY,
            how="left"
        )

    @property
    def name(self) -> str:
        return "Shazam Pre Processor"
