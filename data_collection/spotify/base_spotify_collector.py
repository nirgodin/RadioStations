from abc import ABC, abstractmethod
from typing import Generator, Optional

from aiohttp import ClientSession

from tools.data_chunks_generator import DataChunksGenerator
from utils.spotify_utils import build_spotify_headers


class BaseSpotifyCollector(ABC):
    def __init__(self, session: ClientSession, chunk_size: int, max_chunks_number: Optional[int]):
        self._session = session
        self._chunk_size = chunk_size
        self._max_chunks_number = max_chunks_number
        self._chunks_generator = DataChunksGenerator(chunk_size, max_chunks_number)

    @abstractmethod
    async def collect(self, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _collect_single_chunk(self, chunk: list) -> None:
        raise NotImplementedError

    async def _collect_multiple_chunks(self, lst: list, filtering_list: list) -> None:
        await self._chunks_generator.execute_by_chunk(
            lst=lst,
            filtering_list=filtering_list,
            func=self._collect_single_chunk
        )

    async def _renew_client_session(self) -> None:
        await self._session.close()
        self._session = ClientSession(headers=build_spotify_headers())

    @staticmethod
    def _is_access_token_expired(response: dict) -> bool:
        return response.get('error', {}).get('status') == 401
