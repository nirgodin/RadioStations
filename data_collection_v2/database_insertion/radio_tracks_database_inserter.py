from typing import List

from postgres_client import insert_records
from postgres_client.utils.spotify_utils import extract_artist_id
from sqlalchemy.ext.asyncio import AsyncEngine

from consts.data_consts import ID
from data_collection_v2.database_insertion.base_database_inserter import BaseDatabaseInserter
from database.orm_models.radio_track import RadioTrack


class RadioTracksDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, playlist: dict, tracks: List[dict], artists: List[dict]) -> None:
        records = []

        for track in tracks:
            artist = self._extract_artist_details(track, artists)
            record = RadioTrack.from_playlist_artist_track(
                playlist=playlist,
                artist=artist,
                track=track
            )
            records.append(record)

        await insert_records(engine=self._db_engine, records=records)

    @staticmethod
    def _extract_artist_details(track: dict, artists: List[dict]) -> dict:
        artist_id = extract_artist_id(track)

        for artist in artists:
            if artist[ID] == artist_id:
                return artist
