import asyncio
from typing import List

import pandas as pd
from genie_datastores.postgres.models import ShazamTopTrack, ShazamTrack
from genie_datastores.postgres.operations import get_database_engine, insert_records, execute_query
from pandas import Series, DataFrame
from genie_datastores.postgres.tools import ShazamWritersExtractor
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from consts.audio_features_consts import KEY
from consts.data_consts import ARTIST_ID
from consts.path_consts import SHAZAM_TRACKS_ABOUT_PATH, SHAZAM_ARTISTS_IDS_PATH
from consts.shazam_consts import LABEL, TITLE, FOOTER, PRIMARY_GENRE
from tools.environment_manager import EnvironmentManager
from utils.file_utils import read_json, to_json
from utils.general_utils import stringify_float

ERRORS_PATH = r"database/shazam_tracks_errors.json"


class ShazamTracksMigrationScript:
    def __init__(self):
        self._db_engine = get_database_engine()

    async def run(self):
        tracks_data = pd.read_csv(SHAZAM_TRACKS_ABOUT_PATH)
        artists_ids_data = pd.read_csv(SHAZAM_ARTISTS_IDS_PATH).drop_duplicates(subset=[KEY])
        data = tracks_data.merge(
            right=artists_ids_data,
            how='left',
            on=[KEY]
        )
        data.dropna(subset=[ARTIST_ID], inplace=True)
        filtered_data = await self._filter_existing_records(data)
        print("Starting to create ShazamTrack records")

        with tqdm(total=len(filtered_data)) as progress_bar:
            records = [self._to_record(row, progress_bar) for _, row in filtered_data.iterrows()]

        await self._insert_records(records)

    @staticmethod
    def _to_record(row: Series, progress_bar: tqdm) -> ShazamTrack:
        progress_bar.update(1)
        return ShazamTrack(
            id=stringify_float(row[KEY]),
            artist_id=stringify_float(row[ARTIST_ID]),
            name=row[TITLE],
            label=None if pd.isna(row[LABEL]) else row[LABEL],
            writers=None if pd.isna(row[FOOTER]) else ShazamWritersExtractor._extract_writers_from_footer(row[FOOTER]),
            primary_genre=None if pd.isna(row[PRIMARY_GENRE]) else row[PRIMARY_GENRE],
        )

    async def _insert_records(self, records: List[ShazamTrack]) -> None:
        print("Starting to insert records to database")

        with tqdm(total=len(records)) as progress_bar:
            for record in records:
                await self._insert_single_orm_record_wrapper(record)
                progress_bar.update(1)

    async def _insert_single_orm_record_wrapper(self, record: ShazamTrack) -> None:
        try:
            await insert_records(self._db_engine, [record])

        except IntegrityError:
            print(f"Record already exists in table. Skipping")
            return

        except Exception as e:
            print(f"Received exception!\n{e}")
            self._record_exception(e, record)
            await asyncio.sleep(5)

    @staticmethod
    def _record_exception(e: Exception, record: ShazamTopTrack) -> None:
        exception_record = {
            "error": str(e),
            "details": record.to_dict()
        }
        existing_errors: List[dict] = read_json(ERRORS_PATH)
        existing_errors.append(exception_record)
        to_json(d=existing_errors, path=ERRORS_PATH)

    async def _filter_existing_records(self, data: DataFrame) -> DataFrame:
        data[KEY] = data[KEY].apply(stringify_float)
        data.dropna(subset=KEY, inplace=True)
        query_result = await execute_query(engine=self._db_engine, query=select(ShazamTrack.id))
        ids = query_result.scalars().all()
        filtered_data = data[~data[KEY].isin(ids)]

        return filtered_data.reset_index(drop=True)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ShazamTracksMigrationScript().run())
