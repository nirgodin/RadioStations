import asyncio
from collections import Counter
from typing import List, Tuple, Optional, Dict

import numpy as np
import pandas as pd
from data_collectors.components import ComponentFactory
from genie_common.tools import logger
from pandas import DataFrame

from data_collection.eurovision.eurovision_html_collector import EurovisionHTMLCollector
from data_collection.eurovision.table_details import TableDetails
from utils.file_utils import to_csv


class EurovisionCollector:
    def __init__(self, html_collector: EurovisionHTMLCollector):
        self._html_collector = html_collector

    async def collect(self, years: List[int]) -> None:
        years_to_wiki_pages = await self._html_collector.collect(years)
        voting_results = self._to_voting_results(years_to_wiki_pages)

        to_csv(voting_results, r'data\eurovision\voting_results.csv')
        voting_results.to_clipboard(index=False)

    def _to_voting_results(self, years_to_wiki_pages: List[Tuple[int, str]]) -> DataFrame:
        voting_results = []

        for year, page in years_to_wiki_pages:
            year_results = self._get_single_year_voting_results(year, page)

            if year_results is not None:
                voting_results.append(year_results)

        return pd.concat(voting_results)

    def _get_single_year_voting_results(self, year: int, page: str) -> Optional[DataFrame]:
        logger.info(f"Converting year {year} data to charts entries")
        table_details = self._extract_voting_results_data(page, year)

        if table_details is None:
            logger.warn(f"Could not extract eurovision charts data entries from wikipedia. Skipping year {year} data")
        else:
            return self._pre_process_data(table_details)

    def _extract_voting_results_data(self, page: str, year: int) -> Optional[List[TableDetails]]:
        page_tables = pd.read_html(page)
        relevant_tables = []

        for table in page_tables:
            if table.shape[1] > 10:
                relevant_tables.append(table)

        if relevant_tables:
            return self._generate_table_details(relevant_tables, year)

    def _pre_process_data(self, tables_details: List[TableDetails]) -> DataFrame:
        formatted_data = []

        for table_details in tables_details:
            self._format_column_names(table_details)
            self._format_duplicate_column_names(table_details.data)
            relevant_data = self._filter_relevant_columns(table_details.data)
            self._rename_first_column(relevant_data)
            long_data = relevant_data.melt(
                id_vars=["receiving_country"],
                var_name="giving_country",
                value_name="score"
            )
            long_formatted_data = self._add_metadata_columns(long_data, table_details)
            formatted_data.append(long_formatted_data)

        return pd.concat(formatted_data).reset_index(drop=True)

    def _format_column_names(self, table_details: TableDetails) -> None:
        if self._has_numeric_columns(table_details.data):
            self._drop_irrelevant_rows(table_details)
            table_details.data.drop(0, inplace=True)
            table_details.data.reset_index(drop=True, inplace=True)

        voting_procedure = table_details.data.iloc[0, 1]
        if isinstance(voting_procedure, str) and voting_procedure.lower().__contains__("voting procedure"):
            table_details.data.drop(0, inplace=True)
            table_details.data.reset_index(drop=True, inplace=True)

    @staticmethod
    def _has_numeric_columns(data: DataFrame) -> bool:
        return all(isinstance(column, int) for column in data.columns.tolist())

    @staticmethod
    def _drop_irrelevant_rows(table_details: TableDetails) -> None:
        for i, row in table_details.data.iterrows():
            if any(v == "Televote" for v in row.tolist()):
                table_details.data.drop(i, inplace=True)
                table_details.voting_method = "televote"
            elif any(v == "Jury vote" for v in row.tolist()):
                table_details.data.drop(i, inplace=True)
                table_details.voting_method = "jury"
            else:
                table_details.data.columns = table_details.data.loc[i]
                table_details.data.reset_index(drop=True, inplace=True)
                break

    @staticmethod
    def _filter_relevant_columns(data: DataFrame) -> DataFrame:
        irrelevant_columns = []

        for column in data.columns:
            if column.lower().strip() in ["televoting score", "jury score"]:
                irrelevant_columns.append(column)
            elif column.lower().__contains__("total score"):
                irrelevant_columns.append(column)
            elif "Contestants" in data[column].tolist():  # TODO: Lowercase column values
                irrelevant_columns.append(column)

        return data.drop(irrelevant_columns, axis=1)

    @staticmethod
    def _format_duplicate_column_names(data: DataFrame) -> None:
        column_count = Counter(data.columns.tolist())

        for i, column in enumerate(data.columns):
            if column_count[column] > 1:
                data.columns.values[i] = f"{column}_{i + 1}"

        if np.nan in data.columns:
            data.drop(np.nan, axis=1, inplace=True)

    @staticmethod
    def _rename_first_column(data: DataFrame) -> None:
        first_column = data.columns.tolist()[0]
        data.rename(columns={first_column: "receiving_country"}, inplace=True)

    def _generate_table_details(self, tables: List[DataFrame], year: int) -> List[TableDetails]:
        n_tables = len(tables)
        stages = self._tables_number_stage_mapping[n_tables]
        tables_details = []

        for data, stage in zip(tables, stages):
            details = TableDetails(
                data=data,
                stage=stage,
                year=year
            )
            tables_details.append(details)

        return tables_details

    @property
    def _tables_number_stage_mapping(self) -> Dict[int, List[str]]:
        return {
            1: ["final"],
            2: ["semi_final", "final"],
            3: ["semi_final_1", "semi_final_2", "final"],
            4: ["semi_final_1", "semi_final_2", "final", "final"],
            6: ["semi_final_1", "semi_final_1", "semi_final_2", "semi_final_2", "final", "final"]
        }

    @staticmethod
    def _add_metadata_columns(data: DataFrame, table_details: TableDetails) -> DataFrame:
        data["year"] = table_details.year
        data["stage"] = table_details.stage
        data["voting_method"] = np.nan if table_details.voting_method is None else table_details.voting_method
        data["score"].fillna(0, inplace=True)

        return data.reindex(sorted(data.columns), axis=1)


async def main():
    factory = ComponentFactory()
    async with factory.sessions.get_client_session() as session:
        html_collector = EurovisionHTMLCollector(session, factory.tools.get_pool_executor())
        collector = EurovisionCollector(html_collector)

        await collector.collect([2004])  #list(range(1957, 2024)))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
