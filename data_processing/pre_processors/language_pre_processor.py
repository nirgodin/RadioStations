import pandas as pd
from pandas import DataFrame

from consts.data_consts import SPOTIFY_ID, ID
from consts.path_consts import SHAZAM_TRACKS_IDS_PATH, SHAZAM_TRACKS_LANGUAGES_PATH, MERGED_DATA_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY, APPLE_MUSIC_ADAM_ID
from data_processing.pre_processors.pre_processor_interface import IPreProcessor

SHAZAM_KEY = 'shazam_key'
SHAZAM_ADAMID = 'shazam_adamid'


class LanguagePreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        data_with_shazam_ids = self._merge_shazam_tracks_ids_data(data)
        return self._merge_lyrics_languages_data(data_with_shazam_ids)

    @staticmethod
    def _merge_shazam_tracks_ids_data(data: DataFrame) -> DataFrame:
        shazam_tracks_ids_data = pd.read_csv(SHAZAM_TRACKS_IDS_PATH)
        shazam_tracks_ids_relevant_data = shazam_tracks_ids_data[[SHAZAM_TRACK_KEY, APPLE_MUSIC_ADAM_ID, SPOTIFY_ID]]
        shazam_tracks_ids_relevant_data.columns = [SHAZAM_KEY, SHAZAM_ADAMID, ID]

        return data.merge(
            right=shazam_tracks_ids_relevant_data,
            how='left',
            on=[ID]
       )

    @staticmethod
    def _merge_lyrics_languages_data(data_with_shazam_ids: DataFrame) -> DataFrame:
        lyrics_data = pd.read_csv(SHAZAM_TRACKS_LANGUAGES_PATH)
        lyrics_data.rename(columns={SHAZAM_TRACK_KEY: SHAZAM_KEY}, inplace=True)

        return data_with_shazam_ids.merge(
            right=lyrics_data,
            how='left',
            on=[SHAZAM_KEY]
        )

    @property
    def name(self) -> str:
        return 'language pre processor'


if __name__ == '__main__':
    pre_processor = LanguagePreProcessor()
    pre_processor.pre_process(pd.read_csv(MERGED_DATA_PATH))