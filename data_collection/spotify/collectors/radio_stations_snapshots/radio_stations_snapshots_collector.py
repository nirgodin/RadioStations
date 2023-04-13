import asyncio
from functools import partial
from typing import List, Tuple

from aiohttp import ClientSession
from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import PLAYLIST_URL_FORMAT, AIO_POOL_SIZE, ARTISTS_URL_FORMAT
from consts.data_consts import TRACK, ARTISTS, ID
from consts.playlists_consts import STATIONS
from data_collection.spotify.base_spotify_collector import BaseSpotifyCollector
from data_collection.spotify.collectors.radio_stations_snapshots.artist import Artist
from data_collection.spotify.collectors.radio_stations_snapshots.station import Station
from data_collection.spotify.collectors.radio_stations_snapshots.track import Track
from utils.spotify_utils import build_spotify_headers


class RadioStationsSnapshotsCollector(BaseSpotifyCollector):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        super().__init__(session, chunk_size, max_chunks_number)

    async def collect(self) -> None:
        stations = await self._get_stations_playlists()

        for station in stations:
            station_tracks = await self._get_station_tracks(station)
            print('b')

    async def _collect_single_chunk(self, chunk: list) -> None:
        pass

    async def _get_stations_playlists(self) -> List[Station]:
        pool = AioPool(AIO_POOL_SIZE)
        iterable = list(STATIONS.items())[:2]  # TODO: Remove :2
        progress_bar = tqdm(total=len(iterable))
        func = partial(self._get_single_playlist, progress_bar)

        return await pool.map(func, iterable)

    async def _get_single_playlist(self, progress_bar: tqdm, station_name_and_playlist_id: Tuple[str, str]) -> Station:
        station_name, playlist_id = station_name_and_playlist_id
        url = PLAYLIST_URL_FORMAT.format(playlist_id)

        async with self._session.get(url) as raw_response:
            response = await raw_response.json()
            progress_bar.update(1)

        return Station.from_playlist(station_name=station_name, playlist=response)

    async def _get_station_tracks(self, station: Station) -> List[Track]:
        raw_tracks = [raw_track.get(TRACK, {}) for raw_track in station.tracks]
        artists_ids = self._get_artists_ids(raw_tracks)
        pool = AioPool(AIO_POOL_SIZE)
        progress_bar = tqdm(total=len(artists_ids))
        func = partial(self._get_single_track_artist, progress_bar)
        artists = await pool.map(func, artists_ids)

        return self._serialize_tracks(raw_tracks, artists)

    def _get_artists_ids(self, raw_tracks: List[dict]) -> List[str]:
        artists_ids = []

        for raw_track in raw_tracks:
            artist_id = self._get_single_artist_id(raw_track)
            artists_ids.append(artist_id)

        return artists_ids

    @staticmethod
    def _get_single_artist_id(track: dict) -> str:
        return track.get(ARTISTS, [])[0][ID]  # TODO: Robust

    async def _get_single_track_artist(self, progress_bar: tqdm, artist_id: str) -> Artist:
        url = ARTISTS_URL_FORMAT.format(artist_id)

        async with self._session.get(url) as raw_response:
            response = await raw_response.json()
            progress_bar.update(1)

        return Artist.from_dict(response)

    @staticmethod
    def _serialize_tracks(raw_tracks: List[dict], artists: List[Artist]) -> List[Track]:
        tracks = []

        for raw_track, artist in zip(raw_tracks, artists):
            track = Track.from_raw_track(raw_track, artist)
            tracks.append(track)

        return tracks


if __name__ == '__main__':
    session = ClientSession(headers=build_spotify_headers())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(RadioStationsSnapshotsCollector(session, 1, 1).collect())
