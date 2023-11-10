from abc import ABC, abstractmethod
from typing import List, Type

from postgres_client.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from postgres_client.postgres_operations import execute_query, insert_records
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import ID
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter
from data_collection_v2.database_insertion.base_ids_database_inserter import BaseIDsDatabaseInserter
from tools.logging import logger


class BaseSpotifyDatabaseInserter(BaseIDsDatabaseInserter, ABC):
    @property
    def _serialization_method(self) -> str:
        return "from_spotify_response"
