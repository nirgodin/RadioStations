from typing import List, Optional

from pandas import DataFrame

from analysis.analyzer_interface import IAnalyzer
from analysis.analyzers.gender_analyzer import GenderAnalyzer
from analysis.analyzers.genre_analyzer import GenreAnalyzer
from analysis.analyzers.kan_gimel_analyzer import KanGimelAnalyzer
from analysis.analyzers.lyrics_language.shazam_lyrics_language_analyzer import ShazamLyricsLanguageAnalyzer
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MERGED_DATA_PATH, RADIO_STATIONS_SNAPSHOTS_DIR
from data_processing.data_merger import DataMerger
from data_processing.pre_processors.albums_details_pre_processor import AlbumsDetailsPreProcessor
from data_processing.pre_processors.audio_features_pre_processor import AudioFeaturesPreProcessor
from data_processing.pre_processors.duration_pre_processor import DurationPreProcessor
from data_processing.pre_processors.formatter_pre_processor import FormatterPreProcessor
from data_processing.pre_processors.gender_pre_processor import GenderPreProcessor
from data_processing.pre_processors.genre.genre_pre_processor import GenrePreProcessor
from data_processing.pre_processors.israeli_pre_processor import IsraeliPreProcessor
from data_processing.pre_processors.language_pre_processor import LanguagePreProcessor
from data_processing.pre_processors.pre_processor_interface import IPreProcessor
from data_processing.pre_processors.tracks_ids_pre_processor import TracksIDSPreProcessor
from data_processing.pre_processors.year_pre_processor import YearPreProcessor


class DataPreProcessor:
    def __init__(self, max_year: int):
        self._max_year = max_year

    def pre_process(self, output_path: Optional[str] = None):
        print(f'Starting to merge data to single data frame')
        data = DataMerger.merge(dir_path=RADIO_STATIONS_SNAPSHOTS_DIR, output_path=MERGED_DATA_PATH)
        self._run_pre_script_analyzers()
        pre_processed_data = self._pre_process_data(data)

        if output_path is not None:
            pre_processed_data.to_csv(output_path, encoding=UTF_8_ENCODING, index=False)

        return pre_processed_data

    def _run_pre_script_analyzers(self) -> None:
        for analyzer in self._pre_script_analyzers:
            print(f'Starting to apply {analyzer.name}')
            analyzer.analyze()

    def _pre_process_data(self, data: DataFrame) -> DataFrame:
        pre_processed_data = data.copy(deep=True)

        for pre_processor in self._sorted_pre_processors:
            print(f'Starting to apply {pre_processor.name}')
            pre_processed_data = pre_processor.pre_process(pre_processed_data)

        return pre_processed_data

    @property
    def _sorted_pre_processors(self) -> List[IPreProcessor]:
        return [
            YearPreProcessor(max_year=self._max_year),
            IsraeliPreProcessor(),
            GenrePreProcessor(),
            GenderPreProcessor(),
            AudioFeaturesPreProcessor(),
            TracksIDSPreProcessor(),
            DurationPreProcessor(),
            LanguagePreProcessor(),
            AlbumsDetailsPreProcessor(),
            FormatterPreProcessor()
        ]

    @property
    def _pre_script_analyzers(self) -> List[IAnalyzer]:
        return [
            KanGimelAnalyzer(),
            GenreAnalyzer(),
            ShazamLyricsLanguageAnalyzer(),
            GenderAnalyzer()
        ]
