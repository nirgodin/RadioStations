from abc import ABC, abstractmethod

from aiohttp import ClientSession

from utils.spotify_utils import build_spotify_headers


class BaseSpotifyCollector(ABC):
    def __init__(self, session: ClientSession, chunk_size: int):
        self._session = session
        self._chunk_size = chunk_size

    @abstractmethod
    async def collect(self) -> None:
        raise NotImplementedError

    async def _renew_client_session(self) -> None:
        await self._session.close()
        self._session = ClientSession(headers=build_spotify_headers())

    @staticmethod
    def _is_access_token_expired(response: dict) -> bool:
        return response.get('error', {}).get('status') == 401
