from typing import Iterable, Type

from postgres_client import BillboardTrack, ChartEntryData

from data_collection_v2.database_insertion.base_ids_database_inserter import BaseIDsDatabaseInserter


class BillboardTracksDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, iterable: Iterable[ChartEntryData]) -> Iterable[ChartEntryData]:
        return iterable

    @property
    def _serialization_method(self) -> str:
        return "from_chart_entry"

    @property
    def _orm(self) -> Type[BillboardTrack]:
        return BillboardTrack

    @property
    def name(self) -> str:
        return "billboard_tracks"
