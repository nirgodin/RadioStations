from typing import List, Optional

from postgres_client.models.orm.spotify.spotify_artist import SpotifyArtist
from postgres_client.postgres_operations import execute_query, insert_records
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import TRACK, ARTISTS, ID
from tools.logging import logger

MAX_ARTISTS_PER_REQUEST = 50


class ArtistsCollector:
    def __init__(self, spotify_client: SpotifyClient, db_engine: AsyncEngine):
        self._spotify_client = spotify_client
        self._db_engine = db_engine

    async def collect(self, tracks: List[dict]) -> List[SpotifyArtist]:
        logger.info("Starting executing ArtistsCollector")
        artists_ids = self._get_artists_ids(tracks)
        raw_artists = await self._spotify_client.artists.collect(artists_ids)
        artists = [SpotifyArtist.from_spotify_response(artist) for artist in raw_artists]
        existing_artists_ids = await self._query_existing_artists(artists)
        await self._insert_non_existing_artists(artists, existing_artists_ids)

        return artists

    def _get_artists_ids(self, tracks: List[dict]) -> List[str]:
        artists_ids = []

        for track in tracks:
            artist_id = self._get_single_artist_id(track)

            if artist_id is not None:
                artists_ids.append(artist_id)

        return artists_ids

    @staticmethod
    def _get_single_artist_id(track: dict) -> Optional[str]:
        inner_track = track.get(TRACK, {})
        if inner_track is None:
            return

        artists = inner_track.get(ARTISTS, [])
        if not artists:
            return

        return artists[0][ID]

    async def _query_existing_artists(self, collected_artists: List[SpotifyArtist]) -> List[str]:
        logger.info("Querying database to find existing artists")
        artists_ids = [artist.id for artist in collected_artists]
        query = (
            select(SpotifyArtist.id)
            .where(SpotifyArtist.id.notin_(artists_ids))
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    async def _insert_non_existing_artists(self, artists: List[SpotifyArtist], existing_artists_ids: List[str]):
        non_existing_artists = []

        for artist in artists:
            if artist.id not in existing_artists_ids:
                non_existing_artists.append(artist)

        if non_existing_artists:
            logger.info(f"Inserting {len(non_existing_artists)} artists records to database")
            await insert_records(engine=self._db_engine, records=non_existing_artists)

        self._log_artists_collection_summary(artists, non_existing_artists)

    @staticmethod
    def _log_artists_collection_summary(artists: List[SpotifyArtist], non_existing_artists: List[SpotifyArtist]) -> None:
        number_non_existing = len(non_existing_artists)
        number_existing = len(artists) - number_non_existing

        logger.info(f"Found {number_existing} existing artists and {number_non_existing} non existing artists")
