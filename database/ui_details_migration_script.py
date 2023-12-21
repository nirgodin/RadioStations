import asyncio
import os
from typing import Optional, List
from urllib.parse import unquote, urlparse

import pandas as pd
from data_collectors import ArtistsDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import search_between_two_characters
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import get_database_engine
from genie_datastores.postgres.utils import update_by_values
from pandas import Series

from consts.data_consts import ARTIST_ID
from consts.path_consts import ARTISTS_UI_DETAILS_OUTPUT_PATH
from consts.spotify_ui_consts import FACEBOOK, INSTAGRAM, TWITTER
from consts.wikipedia_consts import WIKIPEDIA
from tools.data_chunks_generator import DataChunksGenerator
from tools.environment_manager import EnvironmentManager


class ArtistsUIDetailsMigrationScript:
    def __init__(self):
        self._chunks_generator = DataChunksGenerator(max_chunks_number=None)
        self._pool_executor = AioPoolExecutor()
        self._db_engine = get_database_engine()

    async def run(self):
        data = pd.read_csv(ARTISTS_UI_DETAILS_OUTPUT_PATH)
        data.dropna(subset=[ARTIST_ID], inplace=True)
        update_requests = [self._to_update_request(row) for _, row in data.iterrows()]

        for chunk in self._chunks_generator.generate_data_chunks(update_requests, filtering_list=[]):
            await self._update(chunk)

    def _to_update_request(self, row: Series) -> DBUpdateRequest:
        values = {
            SpotifyArtist.wikipedia_name: self._extract_wiki_name(row),
            SpotifyArtist.wikipedia_language: self._extract_wiki_language(row),
            SpotifyArtist.about: self._extract_about(row),
            SpotifyArtist.facebook_name: self._extract_social_path(row, FACEBOOK),
            SpotifyArtist.instagram_name: self._extract_social_path(row, INSTAGRAM),
            SpotifyArtist.twitter_name: self._extract_social_path(row, TWITTER)
        }

        return DBUpdateRequest(
            id=row[ARTIST_ID],
            values=values
        )

    @staticmethod
    def _extract_wiki_name(row: Series) -> Optional[str]:
        wikipedia = row[WIKIPEDIA]

        if not pd.isna(wikipedia):
            components = wikipedia.split("/")
            name = components[-1]

            return unquote(name)

    @staticmethod
    def _extract_wiki_language(row: Series) -> Optional[str]:
        wikipedia = row[WIKIPEDIA]

        if not pd.isna(wikipedia):
            matches = search_between_two_characters(
                start_char=r'http',
                end_char=r'\.',
                text=wikipedia
            )
            if matches:
                first_match = matches[0]
                split_match = first_match.split('/')
                language = split_match[-1]

                if language not in ["wikipedia", "www", "ple"]:
                    return language

    @staticmethod
    def _extract_about(row: Series) -> Optional[str]:
        about = row["about"]

        if not pd.isna(about):
            if len(about) <= 5000:
                return about

    @staticmethod
    def _extract_social_path(row: Series, key: str) -> Optional[str]:
        url = row[key]

        if not pd.isna(url):
            parsed_url = urlparse(url)
            return parsed_url.path.strip('/')

    async def _update(self, update_requests: List[DBUpdateRequest]) -> None:
        n_artists = len(update_requests)
        # logger.info(f"Starting to update artists records for {n_artists}")
        results = await self._pool_executor.run(  # TODO: Find a way to do it in Bulk
            iterable=update_requests,
            func=self._update_single_artist,
            expected_type=type(None)
        )

        # logger.info(f"Successfully updated {len(results)} artists records out of {n_artists}")

    async def _update_single_artist(self, update_request: DBUpdateRequest) -> None:
        await update_by_values(
            self._db_engine,
            SpotifyArtist,
            update_request.values,
            SpotifyArtist.id == update_request.id
        )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ArtistsUIDetailsMigrationScript().run())
