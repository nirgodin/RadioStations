from typing import Dict, List, Generator, Union

import pandas as pd
import spacy
from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, LYRICS_NERS_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY
from tools.data_chunks_generator import DataChunksGenerator
from utils.data_utils import extract_column_existing_values
from utils.file_utils import read_json, append_to_csv

NER_TEXTS = 'ner_texts'
NER_LABELS = 'ner_labels'


class NERAnalyzer(IAnalyzer):
    def __init__(self, max_chunks_number: int = 10):
        self._max_chunks_number = max_chunks_number
        self._nlp = spacy.load('en_core_web_lg')
        self._chunks_generator = DataChunksGenerator()
        self._data = self._load_lyrics_data()

    def analyze(self) -> None:
        chunks = self._chunks_generator.generate_data_chunks(
            lst=list(self._data.keys()),
            filtering_list=extract_column_existing_values(LYRICS_NERS_PATH, SHAZAM_TRACK_KEY)
        )
        self._analyze_multiple_chunks(chunks)

    def _analyze_multiple_chunks(self, chunks: Generator[list, None, None]) -> None:
        for chunk_number, chunk in enumerate(chunks):
            if chunk_number + 1 < self._max_chunks_number:
                self._analyze_single_chunk(chunk)
            else:
                break

    def _analyze_single_chunk(self, tracks_ids: List[str]) -> None:
        chunk_lyrics = {track_id: self._data[track_id] for track_id in tracks_ids}
        records = self._get_ners_records(chunk_lyrics)
        data = pd.DataFrame.from_records(records)

        append_to_csv(data, LYRICS_NERS_PATH)

    def _get_ners_records(self, data: Dict[str, List[str]]) -> List[Dict[str, Union[str, List[str]]]]:
        records = []

        with tqdm(total=len(data)) as progress_bar:
            for track_id, track_lyrics in data.items():
                record = self._get_single_ner_record(track_id, track_lyrics)
                records.append(record)
                progress_bar.update(1)

        return records

    def _get_single_ner_record(self, track_id: str, track_lyrics: List[str]) -> Dict[str, Union[str, List[str]]]:
        if not track_lyrics:
            return {
                SHAZAM_TRACK_KEY: track_id
            }
        else:
            concatenated_lyrics = '\n'.join(track_lyrics)
            return self._extract_text_ners(track_id, concatenated_lyrics)

    def _extract_text_ners(self, track_id: str, text: str):
        doc = self._nlp(text)
        record = {
            SHAZAM_TRACK_KEY: track_id,
            NER_TEXTS: [],
            NER_LABELS: []
        }

        for entity in doc.ents:
            record[NER_TEXTS].append(entity.text)
            record[NER_LABELS].append(entity.label_)

        return record

    @staticmethod
    def _load_lyrics_data() -> Dict[str, List[str]]:
        return read_json(SHAZAM_TRACKS_LYRICS_PATH)

    @property
    def name(self) -> str:
        return 'Named entity recognition analyzer'


if __name__ == '__main__':
    NERAnalyzer().analyze()
