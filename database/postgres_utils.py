from typing import Type, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from database.orm_models.base_orm_model import BaseORMModel
from database.postgres_operations import execute_query


async def does_record_exist(engine: AsyncEngine, orm: Type[BaseORMModel], record: BaseORMModel) -> bool:
    primary_key = orm.get_primary_key()
    record_primary_key = getattr(record, primary_key.name)
    query = select(primary_key).where(primary_key == record_primary_key).limit(1)
    result = await execute_query(engine, query)

    return True if result.scalars().first() else False
