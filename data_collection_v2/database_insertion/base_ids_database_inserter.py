from abc import ABC, abstractmethod
from typing import List, Type, Iterable, Any

from genie_datastores.postgres.models import BaseORMModel
from genie_datastores.postgres.operations import execute_query, insert_records
from sqlalchemy import select

from consts.data_consts import ID
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter
from tools.logging import logger


class BaseIDsDatabaseInserter(BaseDatabaseInserter, ABC):
    async def insert(self, iterable: Iterable[Any]) -> List[BaseORMModel]:
        logger.info(f"Starting to run {self.__class__.__name__}")
        raw_records = await self._get_raw_records(iterable)
        records = [getattr(self._orm, self._serialization_method)(record) for record in raw_records]
        unique_records = self._filter_duplicate_ids(records)
        existing_ids = await self._query_existing_ids(unique_records)
        await self._insert_non_existing_records(unique_records, existing_ids)

        return unique_records

    @abstractmethod
    async def _get_raw_records(self, iterable: Iterable[Any]) -> Iterable[Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def _serialization_method(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def _orm(self) -> Type[BaseORMModel]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @staticmethod
    def _filter_duplicate_ids(records: List[BaseORMModel]):
        seen_ids = set()
        filtered_records = []

        for record in records:
            if record.id not in seen_ids:
                seen_ids.add(record.id)
                filtered_records.append(record)

        return filtered_records

    async def _query_existing_ids(self, records: List[BaseORMModel]) -> List[str]:
        logger.info(f"Querying database to find existing ids for table `{self._orm.__tablename__}`")
        records_ids = [record.id for record in records]
        id_column = getattr(self._orm, ID)
        query = (
            select(id_column)
            .where(id_column.in_(records_ids))
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    async def _insert_non_existing_records(self, records: List[BaseORMModel], existing_ids: List[str]) -> None:
        non_existing_records = []

        for record in records:
            if record.id not in existing_ids:
                non_existing_records.append(record)

        if non_existing_records:
            logger.info(f"Inserting {len(non_existing_records)} records to table {self._orm.__tablename__}")
            await insert_records(engine=self._db_engine, records=non_existing_records)

        self._log_summary(records, non_existing_records)

    def _log_summary(self, records: List[BaseORMModel], non_existing_records: List[BaseORMModel]) -> None:
        n_non_exist = len(non_existing_records)
        n_exist = len(records) - n_non_exist
        table_name = self._orm.__tablename__

        logger.info(f"Found {n_exist} existing records and {n_non_exist} non existing records in table `{table_name}`")
