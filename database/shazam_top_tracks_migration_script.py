import asyncio
from datetime import datetime, date
from typing import List

import pandas as pd
from pandas import Series, DataFrame
from genie_datastores.postgres.models import ShazamTopTrack, ShazamLocation
from genie_datastores.postgres.operations import insert_records_ignoring_conflicts, get_database_engine
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from consts.audio_features_consts import KEY
from consts.data_consts import SCRAPED_AT
from consts.datetime_consts import DATETIME_FORMAT
from consts.media_forest_consts import RANK
from consts.path_consts import SHAZAM_CITIES_DIR_PATH, SHAZAM_WORLD_DIR_PATH, SHAZAM_ISRAEL_DIR_PATH
from consts.shazam_consts import LOCATION
from data_processing.data_merger import DataMerger
from tools.environment_manager import EnvironmentManager
from utils.file_utils import read_json, to_json

ERRORS_PATH = r"database/shazam_top_tracks_errors.json"


class ShazamTopTracksMigrationScript:
    def __init__(self):
        self._data_merger = DataMerger(drop_duplicates_on=[KEY, SCRAPED_AT])
        self._db_engine = get_database_engine()

    async def run(self):
        for dir_path in [SHAZAM_CITIES_DIR_PATH, SHAZAM_ISRAEL_DIR_PATH, SHAZAM_WORLD_DIR_PATH]:
            print(f"Starting uploading records from `{dir_path}`")
            await self._migrate_single_location_top_tracks(dir_path)

    async def _migrate_single_location_top_tracks(self, dir_path: str):
        print("Starting merging data")
        data = self._data_merger.merge(dir_path)
        formatted_data = self._add_rank_column(data)

        print("Generating records from data")
        with tqdm(total=len(formatted_data)) as progress_bar:
            records = [self._convert_row_to_record(row, progress_bar) for _, row in formatted_data.iterrows()]

        await self._insert_records(records)

    def _convert_row_to_record(self, row: Series, progress_bar: tqdm) -> ShazamTopTrack:
        progress_bar.update(1)
        return ShazamTopTrack(
            date=self._normalize_date(row[SCRAPED_AT]),
            location=ShazamLocation(row[LOCATION].title()),
            position=row[RANK],
            track_id=str(row[KEY])
        )

    @staticmethod
    def _normalize_date(raw_date: str) -> date:
        date_time = datetime.strptime(raw_date, DATETIME_FORMAT)
        return date_time.date()

    def _add_rank_column(self, data: DataFrame) -> DataFrame:
        print("Adding rank column")
        dfs = []
        unique_locations = data[LOCATION].unique().tolist()
        unique_dates = data[SCRAPED_AT].unique().tolist()

        with tqdm(total=len(unique_dates)) as progress_bar:
            for scrape_date in unique_dates:
                for location in unique_locations:
                    location_data = self._get_single_location_ranked_data(data, scrape_date, location)
                    dfs.append(location_data)

                progress_bar.update(1)

        return pd.concat(dfs).reset_index(drop=True)

    @staticmethod
    def _get_single_location_ranked_data(data: DataFrame, scrape_date: str, location: str) -> DataFrame:
        location_data = data[(data[SCRAPED_AT] == scrape_date) & (data[LOCATION] == location)]
        location_data.reset_index(drop=True, inplace=True)
        location_data[RANK] = location_data.index + 1

        return location_data

    async def _insert_records(self, records: List[ShazamTopTrack]) -> None:
        print("Starting to insert records to database")

        with tqdm(total=len(records)) as progress_bar:
            for record in records:
                await self._insert_single_orm_record_wrapper(record)
                progress_bar.update(1)

    async def _insert_single_orm_record_wrapper(self, record: ShazamTopTrack) -> None:
        try:
            await insert_records_ignoring_conflicts(self._db_engine, [record])

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


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ShazamTopTracksMigrationScript().run())
