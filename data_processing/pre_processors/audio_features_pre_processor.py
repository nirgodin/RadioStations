from pandas import DataFrame

from consts.data_consts import ARTIST_NAME, NAME
from consts.path_consts import AUDIO_FEATURES_BASE_DIR, AUDIO_FEATURES_DATA_PATH
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class AudioFeaturesPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        audio_features = DataMerger.merge(dir_path=AUDIO_FEATURES_BASE_DIR, output_path=AUDIO_FEATURES_DATA_PATH)
        return data.merge(
            right=audio_features,
            how='left',
            on=[NAME, ARTIST_NAME]
        )

    @property
    def name(self) -> str:
        return 'audio features pre processor'
