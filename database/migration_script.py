import asyncio
from functools import partial
from typing import List, Callable, Awaitable, Type

import pandas as pd
from asyncio_pool import AioPool
from pandas import Series
from tqdm import tqdm

from component_factory import ComponentFactory
from consts.path_consts import MERGED_DATA_PATH
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


class DatabaseMigrator:
    def __init__(self):
        EnvironmentManager().set_env_variables()
        self._db_engine = ComponentFactory.get_database_engine()

    async def migrate(self):
        data = pd.read_csv(MERGED_DATA_PATH, nrows=20)  # read_merged_data()
        rows = (row for i, row in data.iterrows())
        pool = AioPool(5)

        with tqdm(total=len(data)) as progress_bar:
            func = partial(self._insert_single_row_records, progress_bar)
            results = await pool.map(func, rows)  # TODO: Add error logging

        print('b')

    async def _insert_single_row_records(self, progress_bar: tqdm, row: Series) -> None:
        for orm in self._ordered_orms:
            try:
                record = orm.from_series(row)
                exists = await does_record_exist(self._db_engine, orm, record)

                if not exists:
                    await insert_records(self._db_engine, [record])
            except Exception as e:
                print(e)

        progress_bar.update(1)

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
