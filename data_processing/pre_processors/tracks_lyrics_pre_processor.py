import pandas as pd
from pandas import DataFrame

from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, MERGED_DATA_PATH
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from utils.file_utils import read_json

SHAZAM_LYRICS = 'shazam_lyrics'


class TracksLyricsPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data_with_shazam_lyrics = self._merge_shazam_tracks_lyrics(data)

    def _merge_shazam_tracks_lyrics(self, data: DataFrame) -> DataFrame:
        shazam_lyrics = read_json(SHAZAM_TRACKS_LYRICS_PATH)
        filtered_lyrics = {k: v for k, v in shazam_lyrics.items() if v != []}
        data[SHAZAM_LYRICS] = data[SHAZAM_KEY].map(filtered_lyrics)
        print('b')

    @property
    def name(self) -> str:
        return "tracks lyrics pre processor"


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH, nrows=1000)
    TracksLyricsPreProcessor().pre_process(data)