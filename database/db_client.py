from sqlalchemy.ext.asyncio import AsyncEngine

from database.orm_models.radio_track import RadioTrack
from database.orm_models.spotify_album import SpotifyAlbum
from database.orm_models.spotify_artist import SpotifyArtist
from database.orm_models.spotify_track import SpotifyTrack
from database.table_client import TableClient


class DBClient:
    def __init__(self, engine: AsyncEngine):
        self.radio_tracks = TableClient(orm=RadioTrack, engine=engine)
        self.spotify_tracks = TableClient(orm=SpotifyTrack, engine=engine)
        self.spotify_artists = TableClient(orm=SpotifyArtist, engine=engine)
        self.spotify_albums = TableClient(orm=SpotifyAlbum, engine=engine)
