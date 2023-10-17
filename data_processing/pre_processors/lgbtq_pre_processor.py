import pandas as pd
from pandas import DataFrame

from consts.data_consts import ARTIST_ID, IS_LGBTQ, STATION
from consts.path_consts import SPOTIFY_LGBTQ_PLAYLISTS_OUTPUT_PATH, MERGED_DATA_PATH
from data_processing.pre_processors.pre_processor_interface import IPreProcessor


class LGBTQPreProcessor(IPreProcessor):
    def pre_process(self, data: DataFrame) -> DataFrame:
        lgbtq_data = pd.read_csv(SPOTIFY_LGBTQ_PLAYLISTS_OUTPUT_PATH)
        merged_data = data.merge(
            right=lgbtq_data[[ARTIST_ID, IS_LGBTQ]],
            how="left",
            on=[ARTIST_ID]
        )
        merged_data[IS_LGBTQ] = merged_data[IS_LGBTQ].fillna(False)

        return merged_data

    @property
    def name(self) -> str:
        return "lgbtq pre processor"


if __name__ == '__main__':
    data = pd.read_csv(MERGED_DATA_PATH)
    LGBTQPreProcessor().pre_process(data)
