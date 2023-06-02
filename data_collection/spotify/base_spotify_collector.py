from abc import ABC, abstractmethod
from typing import Generator

from aiohttp import ClientSession

from utils.spotify_utils import build_spotify_headers


class BaseSpotifyCollector(ABC):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: int):
        self._session = session
        self._chunk_size = chunk_size
        self._max_chunks_number = max_chunks_number

    @abstractmethod
    async def collect(self, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _collect_single_chunk(self, chunk: list) -> None:
        raise NotImplementedError

    async def _collect_multiple_chunks(self, chunks: Generator[list, None, None]) -> None:
        for chunk_number, chunk in enumerate(chunks):
            if chunk_number + 1 < self._max_chunks_number:
                await self._collect_single_chunk(chunk)
            else:
                break

    async def _renew_client_session(self) -> None:
        await self._session.close()
        self._session = ClientSession(headers=build_spotify_headers())

    @staticmethod
    def _is_access_token_expired(response: dict) -> bool:
        return response.get('error', {}).get('status') == 401
