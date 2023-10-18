import asyncio
from functools import partial
from typing import List, Callable, Awaitable, Type, Generator

import numpy as np
import pandas as pd
from asyncio_pool import AioPool
from asyncpg import UniqueViolationError
from pandas import Series, DataFrame
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from component_factory import ComponentFactory
from consts.data_consts import RELEASE_DATE, ALBUM_ID, ID, RELEASE_DATE_PRECISION, ALBUM_RELEASE_DATE, STATION, \
    ADDED_AT, NAME, ARTIST_ID, IS_LGBTQ
from consts.path_consts import MERGED_DATA_PATH, ALBUMS_DETAILS_OUTPUT_PATH, SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH, \
    SPOTIFY_ARTISTS_UI_ANALYZER_OUTPUT_PATH, SPOTIFY_LGBTQ_PLAYLISTS_OUTPUT_PATH, TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from database.orm_models.audio_features import AudioFeatures
from database.orm_models.base_orm_model import BaseORMModel
from database.orm_models.radio_track import RadioTrack
from database.orm_models.spotify_album import SpotifyAlbum
from database.orm_models.spotify_artist import SpotifyArtist
from database.orm_models.spotify_track import SpotifyTrack
from database.orm_models.track_id_mapping import TrackIDMapping
from database.postgres_operations import insert_records
from database.postgres_utils import does_record_exist
from tools.environment_manager import EnvironmentManager
from utils.data_utils import read_merged_data

# TODO:
#  3. Add genius pre processor
#  5. Fill all possible primary keys
#  6. Add error logging


class DatabaseMigrator:
    def __init__(self):
        EnvironmentManager().set_env_variables()
        self._db_engine = ComponentFactory.get_database_engine()

    async def migrate(self):
        data = pd.read_csv(MERGED_DATA_PATH, nrows=100)  # read_merged_data()
        rows = self._load_rows(data)
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
        merged_data = data.merge(
            right=pd.read_csv(SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH),
            how='left',
            on=SHAZAM_KEY
        )
        merged_data = merged_data.merge(
            right=pd.read_csv(SPOTIFY_ARTISTS_UI_ANALYZER_OUTPUT_PATH),
            how="left",
            on=ARTIST_ID
        )
        lgbtq_data = pd.read_csv(SPOTIFY_LGBTQ_PLAYLISTS_OUTPUT_PATH)
        merged_data = merged_data.merge(
            right=lgbtq_data[[ARTIST_ID, IS_LGBTQ]],
            how="left",
            on=[ARTIST_ID]
        )
        merged_data[IS_LGBTQ] = merged_data[IS_LGBTQ].fillna(False)
        merged_data = merged_data.merge(
            right=pd.read_csv(TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH).drop(SHAZAM_KEY, axis=1),
            how='left',
            on=[ID]
        )

        merged_data.replace([np.nan], [None], inplace=True)
        merged_data[STATION] = merged_data[STATION].apply(lambda x: '_'.join(x.split(' ')))

        for i, row in merged_data.iterrows():
            yield row

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
            AudioFeatures,
            TrackIDMapping
        ]


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(DatabaseMigrator().migrate())
