from typing import List

from postgres_client import ChartEntryData, BillboardTrack, execute_query
from sqlalchemy import update, case
from sqlalchemy.ext.asyncio import AsyncEngine


class BillboardTracksUpdater:
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def update(self, charts_entries: List[ChartEntryData]) -> None:
        ids_weeks_mapping = {entry.id: entry.entry.weeks for entry in charts_entries}
        update_query = (
            update(BillboardTrack)
            .where(BillboardTrack.id.in_(ids_weeks_mapping.keys()))
            .values(
                {
                    BillboardTrack.weeks_on_chart: case(
                        ids_weeks_mapping, value=BillboardTrack.id
                    )
                }
            )
        )
        await execute_query(engine=self._db_engine, query=update_query)
