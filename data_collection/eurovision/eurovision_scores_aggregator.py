import pandas as pd
from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler

from utils.file_utils import to_csv

GROUP_BY_COLUMNS = ["year", "voting_method", "stage", "receiving_country"]


class EurovisionScoresAggregator:
    def aggregate(self):
        data = pd.read_csv(r"data/eurovision/voting_results.csv")
        aggregated_scores = self._get_aggregated_scores(data)
        total_scores = self._get_aggregated_total_scores(aggregated_scores)
        total_scores["stage_rank"] = total_scores["stage"].map({"final": 1, "semi_final": 0})
        normalized_scores = self._add_normalized_scores(total_scores)
        self._add_all_time_rank_column(normalized_scores)

        to_csv(normalized_scores, r'data/eurovision/normalized_voting_scores.csv')

    @staticmethod
    def _get_aggregated_scores(data: DataFrame) -> DataFrame:
        data.rename(columns={"stage": "event"}, inplace=True)
        data["stage"] = data["event"].apply(lambda x: "semi_final" if x.__contains__("semi") else "final")
        data.dropna(subset=["receiving_country"], inplace=True)
        data["receiving_country"] = data["receiving_country"].apply(lambda x: x.replace("‡", "").strip())
        data["voting_method"].fillna("aggregated", inplace=True)
        aggregated_data = data.groupby(GROUP_BY_COLUMNS).sum("score")
        levels = list(range(len(GROUP_BY_COLUMNS)))
        aggregated_data.reset_index(level=levels, inplace=True)
        to_csv(aggregated_data, r'data/eurovision/aggregated_voting_results.csv')

        return aggregated_data

    @staticmethod
    def _get_aggregated_total_scores(data: DataFrame) -> DataFrame:
        non_voting_method_columns = [col for col in GROUP_BY_COLUMNS if col != "voting_method"]
        levels = list(range(len(non_voting_method_columns)))
        aggregated_data = data.groupby(non_voting_method_columns).sum("score")
        aggregated_data.reset_index(level=levels, inplace=True)

        return aggregated_data

    def _add_normalized_scores(self, data: DataFrame) -> DataFrame:
        normalized_scores = []

        for year in data["year"].unique():
            normalized_data = self._normalize_single_year_scores(data, year)
            normalized_scores.append(normalized_data)

        return pd.concat(normalized_scores).reset_index(drop=True)

    def _normalize_single_year_scores(self, data: DataFrame, year: int) -> DataFrame:
        normalized_scores = []
        year_data = data[data["year"] == year]

        for stage in year_data["stage"].unique():
            normalized_data = self._normalize_single_stage_scores(year_data, stage)
            normalized_scores.append(normalized_data)

        return pd.concat(normalized_scores).reset_index(drop=True)

    def _normalize_single_stage_scores(self, data: DataFrame, stage: str) -> DataFrame:
        stage_data = data[data["stage"] == stage]
        scaler = MinMaxScaler()
        formatted_scores = stage_data["score"].to_numpy().reshape(-1, 1)
        stage_data["normalized_score"] = scaler.fit_transform(formatted_scores)
        stage_data["normalized_score"] = stage_data["normalized_score"].apply(lambda x: 1 if x > 0.999 else x)
        self._add_rank_column(stage_data)
        self._add_gap_from_second_column(stage_data)

        return stage_data

    @staticmethod
    def _add_rank_column(data: DataFrame) -> None:
        data.sort_values(by="score", ascending=False, inplace=True)
        data.reset_index(drop=True, inplace=True)
        data["rank"] = data.index + 1
        data["sort_rank"] = data["rank"] * -1

    @staticmethod
    def _add_gap_from_second_column(data: DataFrame) -> None:
        second_place_data = data[data["rank"] == 2].reset_index(drop=True)
        second_place_score = second_place_data.at[0, "normalized_score"]
        data["gap_from_second"] = data["normalized_score"] - second_place_score

    @staticmethod
    def _add_all_time_rank_column(data: DataFrame) -> None:
        data.sort_values(by=["stage_rank", "sort_rank", "normalized_score", "gap_from_second"], ascending=False, inplace=True)
        data.reset_index(drop=True, inplace=True)
        data["all_time_rank"] = data.index + 1


if __name__ == '__main__':
    EurovisionScoresAggregator().aggregate()
