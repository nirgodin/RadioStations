import pandas as pd
from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from consts.data_consts import NAME, ARTIST_NAME
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import AUDIO_FEATURES_BASE_DIR, MERGED_DATA_PATH
from data_processing.data_merger import DataMerger


class AudioFeaturesMissingTracksAnalyzer(IAnalyzer):
    def __init__(self, output_path: str):
        self._output_path = output_path

    def analyze(self) -> None:
        audio_features_data = DataMerger.merge(
            dir_path=AUDIO_FEATURES_BASE_DIR,
            drop_duplicates_on=[NAME, ARTIST_NAME]
        )
        full_tracks_data = pd.read_csv(MERGED_DATA_PATH)
        unique_tracks_data = full_tracks_data.drop_duplicates(subset=[NAME, ARTIST_NAME])
        missing_audio_features = self._extract_missing_audio_features_tracks(unique_tracks_data, audio_features_data)

        missing_audio_features.to_csv(self._output_path, index=False, encoding=UTF_8_ENCODING)

    @staticmethod
    def _extract_missing_audio_features_tracks(tracks_data: DataFrame, audio_features_data: DataFrame) -> DataFrame:
        merged_data = tracks_data.merge(
            right=audio_features_data,
            how='left',
            on=[ARTIST_NAME, NAME],
            indicator=True
        )
        missing_audio_features_data = merged_data[merged_data['_merge'] == 'left_only']

        return missing_audio_features_data[[ARTIST_NAME, NAME]]

    @property
    def name(self) -> str:
        return 'audio features missing tracks analyzer'


if __name__ == '__main__':
    AudioFeaturesMissingTracksAnalyzer(r'C:\Users\nirgo\Documents\spotify_merged_data_31_12_22.csv').analyze()
