from analysis.analyzer_interface import IAnalyzer
from consts.audio_features_consts import KEY
from consts.data_consts import SCRAPED_AT, NAME, ARTIST_NAME
from consts.media_forest_consts import RANK
from consts.path_consts import SHAZAM_ISRAEL_DIR_PATH, SHAZAM_ISRAEL_MERGED_DATA
from consts.shazam_consts import SHAZAM_RANK, TITLE, SUBTITLE
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from utils.file_utils import to_csv

SHAZAM_COLUMNS_MAPPING = {
    KEY: SHAZAM_KEY,
    RANK: SHAZAM_RANK,
    TITLE: NAME,
    SUBTITLE: ARTIST_NAME
}


class ShazamAnalyzer(IAnalyzer):
    def __init__(self):
        self._data_merger = DataMerger(drop_duplicates_on=[KEY, SCRAPED_AT])

    def analyze(self) -> None:
        data = self._data_merger.merge(SHAZAM_ISRAEL_DIR_PATH)
        data.rename(columns=SHAZAM_COLUMNS_MAPPING, inplace=True)

        to_csv(data, SHAZAM_ISRAEL_MERGED_DATA)

    @property
    def name(self) -> str:
        return "Shazam analyzer"
