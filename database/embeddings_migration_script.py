import asyncio
import os
from time import sleep
from typing import List

import pandas as pd
from genie_datastores.milvus.collections import track_names_embeddings
from genie_datastores.milvus.operations import insert_records, milvus_session, create_collection
from genie_datastores.postgres.models import SpotifyTrack
from genie_datastores.postgres.operations import get_database_engine, execute_query
from pandas import Series
from pymilvus.orm import utility
from sqlalchemy import update
from tqdm import tqdm

from consts.data_consts import ID, NAME, URI
from consts.path_consts import TRACK_NAMES_EMBEDDINGS_PATH
from tools.data_chunks_generator import DataChunksGenerator
from tools.environment_manager import EnvironmentManager
from utils.file_utils import read_json, to_json

ERRORS_PATH = r"database/embeddings_errors.json"


class EmbeddingsMigrator:
    def __init__(self):
        self._chunks_generator = DataChunksGenerator(max_chunks_number=None)
        self._db_engine = get_database_engine()

    async def migrate(self):
        with milvus_session():
            utility.drop_collection(track_names_embeddings.name)

        create_collection(track_names_embeddings)

        data = pd.read_csv(TRACK_NAMES_EMBEDDINGS_PATH)
        data.dropna(inplace=True)
        chunks = self._chunks_generator.generate_data_chunks(
            lst=[row for _, row in data.iterrows()],
            filtering_list=[]
        )
        n_chunks = round(len(data) / 50)

        with tqdm(total=n_chunks) as progress_bar:
            for i, chunk in enumerate(chunks):
                self._insert_single_chunk_wrapper(i, chunk)
                await self._mark_uploaded_embeddings_wrapper(i, chunk)
                progress_bar.update(1)

    async def _mark_uploaded_embeddings_wrapper(self, chunk_number: int, chunk: List[Series]) -> None:
        try:
            await self._mark_uploaded_embeddings_in_postgres(chunk)

        except Exception as e:
            print(f"Received exception! {e}")
            self._record_exception(e, chunk_number)
            sleep(5)

    async def _mark_uploaded_embeddings_in_postgres(self, chunk: List[Series]) -> None:
        ids = [row[ID] for row in chunk]
        query = (
            update(SpotifyTrack)
            .where(SpotifyTrack.id.in_(ids))
            .values({SpotifyTrack.has_name_embeddings: True})
        )
        await execute_query(engine=self._db_engine, query=query)

    def _insert_single_chunk_wrapper(self, chunk_number: int, chunk: List[Series]) -> None:
        try:
            self._insert_single_chunk(chunk)

        except Exception as e:
            print(f"Received exception! {e}")
            self._record_exception(e, chunk_number)
            sleep(5)

    @staticmethod
    def _insert_single_chunk(chunk: List[Series]) -> None:
        data = pd.DataFrame(chunk)
        records = [
            data[ID].tolist(),
            data[NAME].tolist(),
            data.drop([ID, NAME, URI], axis=1).to_numpy()
        ]
        insert_records(config=track_names_embeddings, records=records)

    @staticmethod
    def _record_exception(e: Exception, chunk_number: int) -> None:
        exception_record = {
            "error": str(e),
            "chunk_number": chunk_number
        }
        existing_errors: List[dict] = read_json(ERRORS_PATH)
        existing_errors.append(exception_record)
        to_json(d=existing_errors, path=ERRORS_PATH)


if __name__ == "__main__":
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(EmbeddingsMigrator().migrate())
