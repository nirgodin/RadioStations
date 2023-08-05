from typing import Tuple, Generator, Optional

from tqdm import tqdm

from consts.data_consts import NAME, ID
from consts.path_consts import MERGED_DATA_PATH, \
    TRACK_NAMES_EMBEDDINGS_PATH
from data_collection.openai.embeddings.base_embeddings_collector import BaseEmbeddingsCollector
from utils.data_utils import extract_column_existing_values


class TrackNamesEmbeddingsCollector(BaseEmbeddingsCollector):
    def __init__(self,
                 chunk_size: int = 50,
                 chunks_limit: Optional[int] = None,
                 output_path: str = TRACK_NAMES_EMBEDDINGS_PATH):
        super().__init__(output_path, chunk_size, chunks_limit)

    def _generate_chunks(self) -> Generator[list, None, None]:
        return self._data_chunks_generator.generate_data_chunks(
            lst=extract_column_existing_values(MERGED_DATA_PATH, [ID, NAME]),
            filtering_list=extract_column_existing_values(TRACK_NAMES_EMBEDDINGS_PATH, [ID, NAME])
        )

    async def _extract_single_embeddings(self, progress_bar: tqdm, track_id_and_name: Tuple[str, str]) -> dict:
        track_id, track_name = track_id_and_name
        record = {NAME: track_name, ID: track_id}
        embeddings_record = await self._get_embeddings_record(track_name)
        record.update(embeddings_record)
        progress_bar.update(1)

        return record
