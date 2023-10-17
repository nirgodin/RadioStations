from typing import Union, List
from urllib.parse import urlparse, unquote

import numpy as np
import pandas as pd

from analysis.analyzer_interface import IAnalyzer
from analysis.analyzers.artists_ui_details.extraction_instructions import ExtractionInstructions
from consts.path_consts import ARTISTS_UI_DETAILS_OUTPUT_PATH, SPOTIFY_ARTISTS_UI_ANALYZER_OUTPUT_PATH
from consts.spotify_ui_consts import INSTAGRAM, INSTAGRAM_NAME, FACEBOOK, FACEBOOK_NAME, TWITTER, TWITTER_NAME, \
    ARTISTS_UI_ANALYZER_OUTPUT_COLUMNS
from consts.wikipedia_consts import WIKIPEDIA_LANGUAGE, WIKIPEDIA, WIKIPEDIA_NAME, LOWERCASED_WIKIPEDIA
from utils.file_utils import to_csv


class SpotifyArtistsUIAnalyzer(IAnalyzer):
    def analyze(self) -> None:
        data = pd.read_csv(ARTISTS_UI_DETAILS_OUTPUT_PATH)

        for instruction in self._extractions_instructions:
            data[instruction.result_column] = data[instruction.source_column].apply(instruction.extraction_method)

        analyzer_output = data[ARTISTS_UI_ANALYZER_OUTPUT_COLUMNS]
        to_csv(data=analyzer_output, output_path=SPOTIFY_ARTISTS_UI_ANALYZER_OUTPUT_PATH)

    def _extract_wikipedia_langauge(self, wikipedia_page: Union[float, str]) -> Union[float, str]:
        if pd.isna(wikipedia_page):
            return np.nan

        parsed_url = urlparse(wikipedia_page)
        split_hostname = parsed_url.hostname.split('.')
        language = split_hostname[0]

        if language == LOWERCASED_WIKIPEDIA:
            return self._extract_language_from_ambiguous_domain(parsed_url.path)

        return language

    @staticmethod
    def _extract_wikipedia_path(wikipedia_page: Union[float, str]) -> Union[float, str]:
        if pd.isna(wikipedia_page):
            return np.nan

        parsed_url = urlparse(wikipedia_page)
        quoted_path = parsed_url.path.replace('/wiki/', '')

        return unquote(quoted_path)

    @staticmethod
    def _extract_language_from_ambiguous_domain(path: str) -> str:
        unquoted_path = unquote(path)
        return "en" if unquoted_path == path else "he"

    @staticmethod
    def _extract_unquoted_name(url: Union[float, str]) -> Union[float, str]:
        if pd.isna(url):
            return np.nan

        parsed_url = urlparse(url)
        unquoted_path = unquote(parsed_url.path)

        return unquoted_path.strip('/')

    @property
    def _extractions_instructions(self) -> List[ExtractionInstructions]:
        return [
            ExtractionInstructions(
                source_column=INSTAGRAM,
                result_column=INSTAGRAM_NAME,
                extraction_method=self._extract_unquoted_name
            ),
            ExtractionInstructions(
                source_column=FACEBOOK,
                result_column=FACEBOOK_NAME,
                extraction_method=self._extract_unquoted_name
            ),
            ExtractionInstructions(
                source_column=TWITTER,
                result_column=TWITTER_NAME,
                extraction_method=self._extract_unquoted_name
            ),
            ExtractionInstructions(
                source_column=WIKIPEDIA,
                result_column=WIKIPEDIA_LANGUAGE,
                extraction_method=self._extract_wikipedia_langauge
            ),
            ExtractionInstructions(
                source_column=WIKIPEDIA,
                result_column=WIKIPEDIA_NAME,
                extraction_method=self._extract_wikipedia_path
            ),
        ]

    @property
    def name(self) -> str:
        return "artists ui analyzer"
