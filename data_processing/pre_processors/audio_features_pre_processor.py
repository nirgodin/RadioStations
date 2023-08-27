from pandas import DataFrame

from consts.audio_features_consts import KEY, KEY_NAMES_MAPPING
from consts.data_consts import ARTIST_NAME, NAME, DURATION_MS, SCRAPED_AT, TYPE, TRACK_HREF, ANALYSIS_URL, \
    TIME_SIGNATURE, ERROR
from consts.path_consts import AUDIO_FEATURES_BASE_DIR, AUDIO_FEATURES_DATA_PATH
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.pre_processor_interface import IPreProcessor

AUDIO_FEATURES_DROP_COLUMNS = [
    DURATION_MS,
    SCRAPED_AT,
    TYPE,
    TRACK_HREF,
    ANALYSIS_URL,
    TIME_SIGNATURE,
    ERROR
]


class AudioFeaturesPreProcessor(IPreProcessor):
    def __init__(self):
        self._data_merger = DataMerger(drop_duplicates_on=[NAME, ARTIST_NAME])

    def pre_process(self, data: DataFrame) -> DataFrame:
        audio_features = self._data_merger.merge(
            dir_path=AUDIO_FEATURES_BASE_DIR,
            output_path=AUDIO_FEATURES_DATA_PATH
        )
        audio_features.drop([DURATION_MS, SCRAPED_AT], axis=1, inplace=True)
        audio_features[KEY] = audio_features[KEY].map(KEY_NAMES_MAPPING)

        return data.merge(
            right=audio_features,
            how='left',
            on=[NAME, ARTIST_NAME]
        )

    @property
    def name(self) -> str:
        return 'audio features pre processor'
