from functools import partial
from typing import List, Dict, Tuple

from aiohttp import ClientSession
from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE, PLAYLIST_URL_FORMAT
from data_collection.spotify.collectors.radio_stations_snapshots.data_classes.playlist import Playlist


class PlaylistsCollector:
    def __init__(self, session: ClientSession):
        self._session = session

    async def collect(self, playlists: Dict[str, str]) -> List[Playlist]:
        print('Starting to collect playlists')
        pool = AioPool(AIO_POOL_SIZE)
        iterable = list(playlists.items())
        progress_bar = tqdm(total=len(iterable))
        func = partial(self._get_single_playlist, progress_bar)

        return await pool.map(func, iterable)

    async def _get_single_playlist(self, progress_bar: tqdm, station_name_and_playlist_id: Tuple[str, str]) -> Playlist:
        station_name, playlist_id = station_name_and_playlist_id
        url = PLAYLIST_URL_FORMAT.format(playlist_id)

        async with self._session.get(url) as raw_response:
            response = await raw_response.json()
            progress_bar.update(1)

        return Playlist.from_spotify_response(station_name=station_name, playlist=response)
