from typing import List

from postgres_client import BillboardChartEntry, ChartEntryData, insert_records

from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter


class BillboardChartsDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, charts_entries: List[ChartEntryData]) -> List[BillboardChartEntry]:
        records = [BillboardChartEntry.from_chart_entry(entry) for entry in charts_entries]
        await insert_records(engine=self._db_engine, records=records)

        return records
