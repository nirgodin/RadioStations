import asyncio
from functools import partial
from typing import List, Callable, Awaitable, Type, Generator

import pandas as pd
from asyncio_pool import AioPool
from asyncpg import UniqueViolationError
from pandas import Series, DataFrame
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from component_factory import ComponentFactory
from consts.data_consts import RELEASE_DATE, ALBUM_ID, ID, RELEASE_DATE_PRECISION, ALBUM_RELEASE_DATE, STATION, \
    ADDED_AT, NAME
from consts.path_consts import MERGED_DATA_PATH, ALBUMS_DETAILS_OUTPUT_PATH
from database.orm_models.audio_features import AudioFeatures
from database.orm_models.base_orm_model import BaseORMModel
from database.orm_models.radio_track import RadioTrack
from database.orm_models.spotify_album import SpotifyAlbum
from database.orm_models.spotify_artist import SpotifyArtist
from database.orm_models.spotify_track import SpotifyTrack
from database.postgres_operations import insert_records
from database.postgres_utils import does_record_exist
from tools.environment_manager import EnvironmentManager
from utils.data_utils import read_merged_data

# TODO:
#  1. Add LGBTQ pre processor
#  2. Add spotify UI pre processor
#  3. Fill all possible primary keys
#  2. Add error logging


class DatabaseMigrator:
    def __init__(self):
        EnvironmentManager().set_env_variables()
        self._db_engine = ComponentFactory.get_database_engine()

    async def migrate(self):
        data = pd.read_csv(MERGED_DATA_PATH, nrows=20)  # read_merged_data()
        rows = [row for i, row in data.iterrows()]  # self._load_rows(data)
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

    # @staticmethod
    # def _load_rows(data: DataFrame) -> Generator[Series, None, None]:
    #     albums_data = pd.read_csv(ALBUMS_DETAILS_OUTPUT_PATH)
    #     albums_data.rename(columns={RELEASE_DATE: ALBUM_RELEASE_DATE, ID: ALBUM_ID}, inplace=True)
    #     merged_data = data.merge(
    #         right=albums_data[[ALBUM_ID, ALBUM_RELEASE_DATE, RELEASE_DATE_PRECISION, "total_tracks"]],
    #         how='left',
    #         on=ALBUM_ID
    #     )
    #     merged_data.drop_duplicates(subset=[NAME, ADDED_AT, STATION], inplace=True)
    #
    #     for i, row in merged_data.iterrows():
    #         yield row

    async def _insert_single_row_records(self, progress_bar: tqdm, row: Series) -> None:
        for orm in self._ordered_orms:
            await self._insert_single_orm_record_wrapper(orm, row)

        progress_bar.update(1)

    async def _insert_single_orm_record_wrapper(self, orm: Type[BaseORMModel], row: Series, retries_left: int = 2) -> None:
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
            await asyncio.sleep(5)
            await self._insert_single_orm_record_wrapper(orm, row, retries_left - 1)

    async def _insert_single_orm_record(self, orm: Type[BaseORMModel], row: Series) -> None:
        record = orm.from_series(row)
        exists = await does_record_exist(self._db_engine, orm, record)

        if not exists:
            await insert_records(self._db_engine, [record])

    @property
    def _ordered_orms(self) -> List[Type[BaseORMModel]]:
        return [
            SpotifyArtist,
            SpotifyAlbum,
            SpotifyTrack,
            RadioTrack,
            AudioFeatures
        ]


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(DatabaseMigrator().migrate())
