import os
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from wikipediaapi import Wikipedia

from consts.env_consts import DATABASE_URL
from database.db_client import DBClient


class ComponentFactory:
    @staticmethod
    @lru_cache
    def get_wikipedia(language_abbreviation: str) -> Wikipedia:
        return Wikipedia(language_abbreviation)

    @staticmethod
    @lru_cache
    def get_db_client() -> DBClient:
        engine = ComponentFactory.get_database_engine()
        return DBClient(engine)

    @staticmethod
    @lru_cache
    def get_database_engine() -> AsyncEngine:
        url = os.environ[DATABASE_URL]
        return create_async_engine(url=url, connect_args={})
