import asyncio
from datetime import datetime
from functools import partial
from typing import List, Type, Generator

import numpy as np
import pandas as pd
from asyncio_pool import AioPool
from pandas import Series, DataFrame
from postgres_client.postgres_operations import insert_records, execute_query
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from component_factory import ComponentFactory
from consts.data_consts import STATION, \
    ADDED_AT, NAME
from consts.datetime_consts import SPOTIFY_DATETIME_FORMAT
from consts.path_consts import MERGED_DATA_PATH
from database.orm_models.audio_features import AudioFeatures
from database.orm_models.base_orm_model import BaseORMModel
from database.orm_models.radio_track import RadioTrack
from database.orm_models.spotify_album import SpotifyAlbum
from database.orm_models.spotify_artist import SpotifyArtist
from database.orm_models.spotify_track import SpotifyTrack
from database.orm_models.track_id_mapping import TrackIDMapping
from database.orm_models.track_lyrics import TrackLyrics
from database.postgres_utils import does_record_exist
from tools.environment_manager import EnvironmentManager
from utils.data_utils import read_merged_data
from utils.file_utils import read_json, to_json

ERRORS_PATH = r"database/errors.json"
# TODO:
#  3. Add genius pre processor
#  5. Fill all possible primary keys


class DatabaseMigrator:
    def __init__(self):
        EnvironmentManager().set_env_variables()
        self._db_engine = ComponentFactory.get_database_engine()

    async def migrate(self):
        print("Starting to insert records to database")
        data = read_merged_data()
        filtered_data = await self._filter_non_existing_records(data)

        if filtered_data.empty:
            print("Did not find any non existing record. Aborting")
            return

        rows = self._load_rows(filtered_data)
        await self._insert_non_existing_records(filtered_data, rows)

    async def _insert_non_existing_records(self, data: DataFrame, rows: Generator[Series, None, None]) -> None:
        pool = AioPool(5)

        with tqdm(total=len(data)) as progress_bar:
            func = partial(self._insert_single_row_records, progress_bar)
            results = await pool.map(func, rows)

        error_count = 0
        success_count = 0

        for result in results:
            if result is None:
                success_count += 1
            else:
                error_count += 1

        print(f"Success: {success_count}. Errors: {error_count}")

    @staticmethod
    def _load_rows(data: DataFrame) -> Generator[Series, None, None]:
        data.replace([np.nan], [None], inplace=True)
        data[STATION] = data[STATION].apply(lambda x: '_'.join(x.split(' ')))

        for i, row in data.iterrows():
            yield row

    async def _filter_non_existing_records(self, data: DataFrame) -> DataFrame:
        query = (
            select(RadioTrack.added_at)
            .order_by(RadioTrack.added_at.desc())
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        last_added_at: datetime = query_result.first().added_at

        return data[data[ADDED_AT] > last_added_at.strftime(SPOTIFY_DATETIME_FORMAT)]

    async def _insert_single_row_records(self, progress_bar: tqdm, row: Series) -> None:
        for orm in self._ordered_orms:
            await self._insert_single_orm_record_wrapper(orm, row)

        progress_bar.update(1)

    async def _insert_single_orm_record_wrapper(self, orm: Type[BaseORMModel], row: Series, retries_left: int = 1) -> None:
        if retries_left == 0:
            print(f"Could not insert record for orm `{orm.__name__}`. Skipping")
            return

        try:
            await self._insert_single_orm_record(orm, row)

        except IntegrityError:
            print(f"Record already exists in table. Skipping")
            return

        except Exception as e:
            print(f"Received exception!\n{e}")
            self._record_exception(e, row, orm)
            await asyncio.sleep(5)
            await self._insert_single_orm_record_wrapper(orm, row, retries_left - 1)

    async def _insert_single_orm_record(self, orm: Type[BaseORMModel], row: Series) -> None:
        record = orm.from_series(row)
        exists = await does_record_exist(self._db_engine, orm, record)

        if not exists:
            await insert_records(self._db_engine, [record])

    @staticmethod
    def _record_exception(e: Exception, row: Series, orm: Type[BaseORMModel]) -> None:
        exception_record = {
            "error": str(e),
            "name": row[NAME],
            "added_at": row[ADDED_AT],
            "station": row[STATION],
            "table": orm.__tablename__
        }
        existing_errors: List[dict] = read_json(ERRORS_PATH)
        existing_errors.append(exception_record)
        to_json(d=existing_errors, path=ERRORS_PATH)

    @property
    def _ordered_orms(self) -> List[Type[BaseORMModel]]:
        return [
            SpotifyArtist,
            SpotifyAlbum,
            SpotifyTrack,
            RadioTrack,
            AudioFeatures,
            TrackIDMapping,
            TrackLyrics
        ]


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(DatabaseMigrator().migrate())
