from typing import List

from postgres_client import BillboardChartEntry, ChartEntryData, insert_records

from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter
from tools.logging import logger


class BillboardChartsDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, charts_entries: List[ChartEntryData]) -> List[BillboardChartEntry]:
        logger.info(f"Starting to insert {len(charts_entries)} Billboard chart entries")
        records = [BillboardChartEntry.from_chart_entry(entry) for entry in charts_entries]
        await insert_records(engine=self._db_engine, records=records)
        logger.info(f"Successfully inserted Billboard chart entries")

        return records
