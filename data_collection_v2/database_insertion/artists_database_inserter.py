from typing import List, Optional, Type

from postgres_client.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from postgres_client.models.orm.spotify.spotify_artist import SpotifyArtist
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import TRACK, ARTISTS, ID
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter


class ArtistsDatabaseInserter(BaseDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        artists_ids = []

        for track in tracks:
            artist_id = self._get_single_artist_id(track)

            if artist_id is not None:
                artists_ids.append(artist_id)

        return await self._spotify_client.artists.info.collect(artists_ids)

    @staticmethod
    def _get_single_artist_id(track: dict) -> Optional[str]:
        inner_track = track.get(TRACK, {})
        if inner_track is None:
            return

        artists = inner_track.get(ARTISTS, [])
        if not artists:
            return

        return artists[0][ID]

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyArtist
