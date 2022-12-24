import pandas as pd
from pandas import DataFrame

from consts.audio_features_consts import AUDIO_FEATURES
from consts.data_consts import ARTIST_NAME, NAME, POPULARITY, DURATION_MS, EXPLICIT, SCRAPED_AT, IS_ISRAELI
from consts.path_consts import AUDIO_FEATURES_BASE_DIR, MERGED_DATA_PATH
from data_processing.data_merger import DataMerger

PLAYS_COUNT = 'count'
CORRELATION_COLUMNS_SUBSET = AUDIO_FEATURES + [
    POPULARITY,
    PLAYS_COUNT,
    EXPLICIT,
    DURATION_MS,
    IS_ISRAELI
]


class AudioFeaturesCorrelationsAnalyzer:
    def __init__(self):
        self._tracks_data = pd.read_csv(MERGED_DATA_PATH)

    def analyze(self):
        merged_data = self._merge_tracks_and_audio_features_data()
        audio_features_and_popularity = merged_data[CORRELATION_COLUMNS_SUBSET]
        correlations = audio_features_and_popularity.corr()
        correlations[POPULARITY].to_frame().sort_values(by=POPULARITY, ascending=False)

    def _merge_tracks_and_audio_features_data(self) -> DataFrame:
        unique_track_data = self._tracks_data.drop_duplicates(subset=[NAME, ARTIST_NAME])
        tracks_count = self._get_tracks_count()
        track_data_with_count = unique_track_data.merge(
            right=tracks_count,
            how='left',
            on=[NAME, ARTIST_NAME]
        )

        audio_features_data = DataMerger.merge(dir_path=AUDIO_FEATURES_BASE_DIR, drop_duplicates_on=[NAME, ARTIST_NAME])
        audio_features_data.drop([SCRAPED_AT, DURATION_MS], axis=1, inplace=True)

        return audio_features_data.merge(
            right=track_data_with_count,
            how='left',
            on=[NAME, ARTIST_NAME]
        )

    def _get_tracks_count(self) -> DataFrame:
        groubyed_data = self._tracks_data.groupby(by=[NAME, ARTIST_NAME]).count()
        first_column_name = groubyed_data.columns.tolist()[0]
        subseted_groupbyed_data = groubyed_data[[first_column_name]]
        subseted_groupbyed_data.reset_index(level=0, inplace=True)
        subseted_groupbyed_data.reset_index(level=0, inplace=True)
        subseted_groupbyed_data.columns = [ARTIST_NAME, NAME, PLAYS_COUNT]

        return subseted_groupbyed_data


if __name__ == '__main__':
    AudioFeaturesCorrelationsAnalyzer().analyze()