import asyncio

import pandas as pd
from genie_datastores.postgres.models import ChartEntry, Chart, SpotifyTrack, AudioFeatures
from genie_datastores.postgres.operations import get_database_engine, read_sql
from pandas import DataFrame
from sqlalchemy import select

from tools.environment_manager import EnvironmentManager
from utils.file_utils import to_csv

SONGS_COLUMNS = [
    ChartEntry.date,
    ChartEntry.entry_metadata["Country"].label("country"),
    ChartEntry.key,
    SpotifyTrack.explicit,
    AudioFeatures.key.label("musical_key"),
    AudioFeatures.acousticness,
    AudioFeatures.danceability,
    AudioFeatures.duration_ms,
    AudioFeatures.energy,
    AudioFeatures.instrumentalness,
    AudioFeatures.liveness,
    AudioFeatures.loudness,
    AudioFeatures.mode,
    AudioFeatures.speechiness,
    AudioFeatures.tempo,
    AudioFeatures.time_signature,
    AudioFeatures.valence,
]


class EurovisionSongsQuerier:
    def __init__(self):
        self._db_engine = get_database_engine()

    async def query(self):
        query = (
            select(*SONGS_COLUMNS)
            .where(ChartEntry.track_id == SpotifyTrack.id)
            .where(SpotifyTrack.id == AudioFeatures.id)
            .where(ChartEntry.chart == Chart.EUROVISION)
        )
        data = await read_sql(engine=self._db_engine, query=query)
        processed_data = self._pre_process_data(data.copy())

        to_csv(processed_data, r"data/eurovision/songs.csv")

    @staticmethod
    def _pre_process_data(data: DataFrame) -> DataFrame:
        data["year"] = data["date"].apply(lambda date: date.year)
        data["country"] = data["country"].str.replace(r"\[p\]", "")  # TODO: Handle Czechia Czech Republic duplications
        artist_song_data = data["key"].str.split(" - ", expand=True)
        artist_song_data.columns = ["artist", "song"]
        data.drop(["key", "date"], axis=1, inplace=True)

        return pd.concat([data, artist_song_data], axis=1)


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(EurovisionSongsQuerier().query())