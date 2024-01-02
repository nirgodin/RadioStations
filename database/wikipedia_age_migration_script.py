import asyncio
from datetime import datetime
from typing import List, Optional, Union, Generator

import pandas as pd
from data_collectors import ArtistsDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest, ArtistWikipediaDetails
from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.models import Artist
from genie_datastores.postgres.operations import get_database_engine, execute_query
from pandas import DataFrame
from sqlalchemy import select, or_

from consts.data_consts import ARTIST_NAME, ARTIST_ID
from consts.datetime_consts import DATETIME_FORMAT
from consts.path_consts import WIKIPEDIA_AGE_OUTPUT_PATH, ARTISTS_IDS_OUTPUT_PATH
from consts.wikipedia_consts import BIRTH_DATE, DEATH_DATE
from tools.environment_manager import EnvironmentManager


class WikipediaAgeMigrator:
    def __init__(self):
        self._engine = get_database_engine()
        self._artists_updater = ArtistsDatabaseUpdater(
            db_engine=self._engine,
            pool_executor=AioPoolExecutor(5)
        )

    async def migrate(self):
        data = self._load_data()
        update_requests = self._to_update_requests(data)
        existing_ids = await self._query_existing_ids()
        new_update_requests = list(self._filter_non_existing_ids(existing_ids, update_requests))

        await self._artists_updater.update(new_update_requests)

    @staticmethod
    def _load_data() -> DataFrame:
        data = pd.read_csv(WIKIPEDIA_AGE_OUTPUT_PATH)
        artists_ids_data = pd.read_csv(ARTISTS_IDS_OUTPUT_PATH)
        merged_data = data.merge(
            how="left",
            on=[ARTIST_NAME],
            right=artists_ids_data[[ARTIST_NAME, ARTIST_ID]]
        )
        merged_data.drop_duplicates(subset=[ARTIST_ID], inplace=True)
        merged_data.dropna(subset=[ARTIST_ID], inplace=True)

        return merged_data

    def _to_update_requests(self, data: DataFrame) -> List[DBUpdateRequest]:
        update_requests = []

        for _, row in data.iterrows():
            details = ArtistWikipediaDetails(
                id=row[ARTIST_ID],
                birth_date=self._to_datetime(row[BIRTH_DATE]),
                death_date=self._to_datetime(row[DEATH_DATE])
            )

            if details.death_date is not None or details.birth_date is not None:
                request = details.to_update_request()
                update_requests.append(request)

        return update_requests

    @staticmethod
    def _to_datetime(raw_date: Union[float, str]) -> Optional[datetime]:
        if pd.isna(raw_date):
            return

        return datetime.strptime(raw_date, DATETIME_FORMAT)

    async def _query_existing_ids(self) -> List[str]:
        query = (
            select(Artist.id)
            .where(
                or_(
                    Artist.birth_date.is_not(None),
                    Artist.death_date.is_not(None)
                )
            )
        )
        query_result = await execute_query(engine=self._engine, query=query)

        return query_result.scalars().all()

    @staticmethod
    def _filter_non_existing_ids(existing_ids: List[str],
                                 update_requests: List[DBUpdateRequest]) -> Generator[DBUpdateRequest, None, None]:
        for request in update_requests:
            if request.id not in existing_ids:
                yield request


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(WikipediaAgeMigrator().migrate())
