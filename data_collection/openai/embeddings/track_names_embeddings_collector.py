from typing import Tuple, Generator, Optional, List

import pandas as pd
from tqdm import tqdm

from consts.data_consts import NAME, ID, URI
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
            lst=self._get_track_names_for_unique_ids(),
            filtering_list=extract_column_existing_values(TRACK_NAMES_EMBEDDINGS_PATH, [ID, NAME, URI])
        )

    @staticmethod
    def _get_track_names_for_unique_ids() -> List[Tuple[str, str, str]]:
        data = pd.read_csv(MERGED_DATA_PATH)
        data.drop_duplicates(subset=[ID], inplace=True)

        return list(data[[ID, NAME, URI]].itertuples(index=False, name=None))

    async def _extract_single_embeddings(self, progress_bar: tqdm, track_id_name_and_uri: Tuple[str, str, str]) -> dict:
        track_id, track_name, track_uri = track_id_name_and_uri
        record = {
            NAME: track_name,
            ID: track_id,
            URI: track_uri
        }
        embeddings_record = await self._get_embeddings_record(track_name)
        record.update(embeddings_record)
        progress_bar.update(1)

        return record