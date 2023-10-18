import re
from typing import List, Union, Generator

import numpy as np
import pandas as pd

from analysis.analyzer_interface import IAnalyzer
from consts.audio_features_consts import KEY
from consts.path_consts import SHAZAM_TRACKS_ABOUT_PATH, SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH
from consts.shazam_consts import WRITERS, FOOTER, LABEL, PRIMARY_GENRE
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from utils.file_utils import to_csv


class ShazamTracksAboutAnalyzer(IAnalyzer):
    def __init__(self):
        self._multi_spaces_regex = re.compile(r" +")

    def analyze(self) -> None:
        data = pd.read_csv(SHAZAM_TRACKS_ABOUT_PATH)
        data[WRITERS] = data[FOOTER].apply(self._extract_writers)
        data.rename(columns={KEY: SHAZAM_KEY}, inplace=True)
        relevant_data = data[[SHAZAM_KEY, WRITERS, LABEL, PRIMARY_GENRE]]

        to_csv(data=relevant_data, output_path=SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH)

    def _extract_writers(self, footer: Union[float, str]) -> Union[float, List[str]]:
        if pd.isna(footer):
            return np.nan

        relevant_footer = footer.split('\n')[0]
        split_footer = relevant_footer.split(':')

        if len(split_footer) < 2:
            return np.nan

        return list(self._generate_valid_writers_names(split_footer))

    def _generate_valid_writers_names(self, split_footer: List[str]) -> Generator[str, None, None]:
        writers_element = split_footer[1]

        for writer in writers_element.split(","):
            if self._is_valid_writer(writer):
                yield self._pre_process_raw_writer(writer)

    @staticmethod
    def _is_valid_writer(writer: str) -> bool:
        return any(letter.isalpha() for letter in writer)

    def _pre_process_raw_writer(self, raw_writer: str) -> str:
        stripped_writer = raw_writer.strip()
        return self._multi_spaces_regex.sub(" ", stripped_writer)

    @property
    def name(self) -> str:
        return "shazam tracks about analyzer"
