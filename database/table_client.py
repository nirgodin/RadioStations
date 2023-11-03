from typing import Dict, Optional, List, Union, Tuple, Sequence, Type

import pandas as pd
from pandas import DataFrame
from postgres_client.postgres_operations import execute_query, insert_records
from sqlalchemy import select, Select, update, Update, text, Row
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import InstrumentedAttribute

from database.orm_models.base_orm_model import Base


class TableClient:
    def __init__(self, orm: Type[Base], engine: AsyncEngine):
        self._orm = orm
        self._engine = engine

    async def query(self,
                    filters: Optional[Dict[str, str]] = None,
                    distinct: Optional[List[str]] = None,
                    order_by: Optional[List[str]] = None,
                    limit: Optional[int] = None) -> Sequence[Row]:
        query = self._build_query(filters=filters, distinct=distinct, order_by=order_by, limit=limit)
        query_result = await execute_query(engine=self._engine, query=query)

        return query_result.scalars().all()

    async def query_dataframe(self,
                              filters: Optional[Dict[str, str]] = None,
                              distinct: Optional[List[str]] = None,
                              order_by: Optional[List[str]] = None,
                              limit: Optional[int] = None) -> DataFrame:
        query = self._build_query(filters=filters, distinct=distinct, order_by=order_by, limit=limit)

        async with self._engine.begin() as connection:
            return await connection.run_sync(self._wrap_read_sql, query)

    async def insert(self, records: List[Base]) -> None:
        await insert_records(engine=self._engine, records=records)

    async def update(self,
                     update_values: dict,
                     filters: Optional[Dict[str, str]] = None,
                     returning: Optional[InstrumentedAttribute] = None) -> Sequence[Row]:
        query = self._build_initial_update_query()
        filtered_query = self._add_filters_to_query(query, filters)
        filtered_query_with_values = filtered_query.values(update_values).returning(returning)
        update_result = await execute_query(self._engine, filtered_query_with_values)

        return update_result.scalars().all()

    def _build_query(self,
                     filters: Optional[Dict[str, str]],
                     distinct: Optional[List[str]],
                     order_by: Optional[List[Tuple[str, str]]],
                     limit: Optional[int]) -> Select:
        query = self._build_initial_select_query()
        filtered_query = self._add_filters_to_query(query, filters)
        distinct_query = self._add_distinct_clauses_to_query(filtered_query, distinct)
        ordered_query = self._add_order_by_clauses_to_query(distinct_query, order_by)

        return ordered_query.limit(limit)

    def _build_initial_select_query(self) -> Select:
        return select(self._orm)

    def _build_initial_update_query(self) -> Update:
        return update(self._orm)

    def _add_filters_to_query(self,
                              query: Union[Select, Update],
                              filters: Optional[Dict[str, str]]) -> Union[Select, Update]:
        if filters is not None:
            for query_text in self._filters_creator.create_filters(filters):  # TODO: Create filters creator
                query = query.filter(query_text)

        return query

    @staticmethod
    def _add_distinct_clauses_to_query(query: Select, distinct: Optional[List[str]]) -> Select:
        if distinct is not None:
            distinct_text_clauses = [text(column) for column in distinct]
            query = query.distinct(*distinct_text_clauses)

        return query

    @staticmethod
    def _add_order_by_clauses_to_query(query: Select, order_by: Optional[List[Tuple[str, str]]]) -> Select:
        if order_by is not None:
            order_by_text_clauses = [text(f"{column} {direction}") for column, direction in order_by]
            query = query.order_by(*order_by_text_clauses)

        return query

    @staticmethod
    def _wrap_read_sql(connection, query):
        """This method is wrapped to match connection.run_sync api"""
        return pd.read_sql(query, connection)
