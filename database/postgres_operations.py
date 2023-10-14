from typing import List, Union, Any

from sqlalchemy import Update, Select, Result
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from database.orm_models.base_orm_model import Base


async def insert_records(engine: AsyncEngine, records: List[Base]) -> None:
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        async with session.begin():
            session.add_all(records)


async def execute_query(engine: AsyncEngine, query: Union[Select, Update]) -> Result[Any]:
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        async with session.begin():
            return await session.execute(query)


async def create_all_tables(engine: AsyncEngine) -> None:
    print("Starting to create all schema tables")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_all_tables(engine: AsyncEngine) -> None:
    print("Starting to drop all schema tables")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
