from abc import ABC, abstractmethod

from sqlalchemy.ext.asyncio import AsyncEngine

from database.orm_models.base_orm_model import BaseORMModel


class BaseDatabaseInserter(ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    @abstractmethod
    async def insert(self, *args, **kwargs) -> BaseORMModel:
        raise NotImplementedError
