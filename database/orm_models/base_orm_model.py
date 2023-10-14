import copy
from datetime import datetime
from functools import lru_cache
from typing import List

from pandas import Series
from sqlalchemy import TIMESTAMP, Column, inspect
from sqlalchemy.orm import declarative_base, Mapper

Base = declarative_base()


class BaseORMModel(Base):
    __abstract__ = True

    creation_date = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    update_date = Column(TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    @classmethod
    def from_series(cls, series: Series) -> "BaseORMModel":
        items = {k: v for k, v in series.copy(deep=True).items()}
        pre_processed_items = cls._pre_process_record_items(items)
        record_items = {k: v for k, v in pre_processed_items.items() if k in cls.list_columns(cls)}

        return cls(**record_items)

    def list_columns(self) -> List[str]:
        return [col.name for col in self._inspect(self).columns]

    @classmethod
    def get_primary_key(cls) -> Column:
        return cls._inspect(cls).primary_key[0]

    @lru_cache
    def _inspect(self) -> Mapper:
        return inspect(self)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        return record_items
