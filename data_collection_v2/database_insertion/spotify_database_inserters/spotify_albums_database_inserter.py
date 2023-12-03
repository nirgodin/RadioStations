from typing import List, Optional, Type

from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from genie_datastores.postgres.models import SpotifyAlbum
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel

from consts.data_consts import TRACK, ALBUM, ARTISTS, ID, ALBUMS
from data_collection_v2.database_insertion.spotify_database_inserters.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter


class SpotifyAlbumsDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        albums = []

        for track in tracks:
            album = self._extract_single_album(track)
            artist_id = extract_artist_id(track.get(TRACK, {}))

            if album is not None and artist_id is not None:
                album[ARTISTS][0][ID] = artist_id  # TODO: Robust
                albums.append(album)

        return albums

    @staticmethod
    def _extract_single_album(track: dict) -> Optional[dict]:
        inner_track = track.get(TRACK, {})
        if inner_track is None:
            return

        return inner_track.get(ALBUM)

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyAlbum

    @property
    def name(self) -> str:
        return ALBUMS
