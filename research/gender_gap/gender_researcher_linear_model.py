from functools import reduce

from pandas import DataFrame
from statsmodels.api import add_constant, OLS
from statsmodels.iolib import SimpleTable
from statsmodels.iolib.summary import Summary

from consts.data_consts import ARTIST_NAME, PLAY_COUNT
from consts.gender_researcher_consts import DEPENDENT_VARIABLES, VARIABLE, COEFFICIENT, P_VALUE, CONST, Y, VALUE
import seaborn as sns


class GenderResearcherLinearModel:
    def __init__(self, data: DataFrame):
        self.data = data

    def fit(self, y: str):
        x_columns = [col for col in self.data.columns if col not in [ARTIST_NAME] + DEPENDENT_VARIABLES]
        y_data = self.data[y]
        x = add_constant(self.data[x_columns])
        model = OLS(y_data, x)

        return model.fit()

    def compare_coefficients(self) -> DataFrame:
        return self._compare(parameter=COEFFICIENT)

    def compare_p_values(self) -> DataFrame:
        return self._compare(parameter=P_VALUE)

    @staticmethod
    def plot(comparison_data: DataFrame, order_by: str = PLAY_COUNT):
        non_const_data = comparison_data[comparison_data[VARIABLE] != CONST]
        order = non_const_data.sort_values(by=order_by)[VARIABLE]
        melted_data = non_const_data.melt(id_vars=[VARIABLE], var_name=Y)
        g = sns.FacetGrid(melted_data, col=Y, sharex=False, height=5, aspect=1, col_wrap=3, hue=Y)
        g.map(sns.barplot, VALUE, VARIABLE, order=order)

        return g

    def _compare(self, parameter: str) -> DataFrame:
        data = []

        for y in DEPENDENT_VARIABLES:
            model = self.fit(y)
            model_parameter = self._get_model_parameter(summary=model.summary(), y=y, parameter=parameter)
            data.append(model_parameter)

        return reduce(lambda df1, df2: df1.merge(right=df2, on=VARIABLE, how="left"), data)

    def _get_model_parameter(self, summary: Summary, y: str, parameter: str) -> DataFrame:
        data = self._to_dataframe(summary.tables[1])
        parameter_data = data[[VARIABLE, parameter]]
        parameter_data[parameter] = parameter_data[parameter].astype(float)

        return parameter_data.rename(columns={parameter: y})

    @staticmethod
    def _to_dataframe(table: SimpleTable) -> DataFrame:
        df = DataFrame(table.data)
        df.at[0, 0] = VARIABLE
        df.columns = df.iloc[0]
        df.drop(0, inplace=True)

        return df.reset_index(drop=True)
