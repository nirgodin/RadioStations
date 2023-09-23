from typing import List, Dict, Generator, Optional

from tqdm import tqdm

from consts.path_consts import SHAZAM_TRACKS_LYRICS_PATH, SHAZAM_LYRICS_EMBEDDINGS_PATH
from consts.shazam_consts import SHAZAM_TRACK_KEY
from data_collection.openai.embeddings.base_embeddings_collector import BaseEmbeddingsCollector
from utils.data_utils import extract_column_existing_values
from utils.file_utils import read_json


class ShazamLyricsEmbeddingsCollector(BaseEmbeddingsCollector):
    def __init__(self,
                 chunk_size: int = 50,
                 chunks_limit: Optional[int] = None,
                 output_path: str = SHAZAM_LYRICS_EMBEDDINGS_PATH):
        super().__init__(output_path, chunk_size, chunks_limit)
        self._shazam_tracks_lyrics: Dict[str, List[str]] = read_json(SHAZAM_TRACKS_LYRICS_PATH)

    async def collect(self) -> None:
        await self._data_chunks_generator.execute_by_chunk(
            lst=list(self._shazam_tracks_lyrics.keys()),
            filtering_list=self._get_existing_tracks_ids(),
            func=self._extract_single_chunk_embeddings
        )

    @staticmethod
    def _get_existing_tracks_ids() -> List[str]:
        tracks_keys = extract_column_existing_values(SHAZAM_LYRICS_EMBEDDINGS_PATH, SHAZAM_TRACK_KEY)
        return [str(int(track_key)) for track_key in tracks_keys]

    async def _extract_single_embeddings(self, progress_bar: tqdm, track_id: str) -> dict:
        track_lyrics = self._shazam_tracks_lyrics[track_id]
        record = {SHAZAM_TRACK_KEY: track_id}

        if not track_lyrics:
            return record

        track_concatenated_lyrics = '\n'.join(track_lyrics)
        embeddings_record = await self._get_embeddings_record(track_concatenated_lyrics)
        record.update(embeddings_record)
        progress_bar.update(1)

        return record
