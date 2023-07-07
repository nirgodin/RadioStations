import os
from abc import abstractmethod, ABC
from typing import Optional, Generator, List

from aiohttp import ClientSession

from consts.env_consts import GENIUS_CLIENT_ACCESS_TOKEN
from consts.genius_consts import META, STATUS
from tools.data_chunks_generator import DataChunksGenerator


class BaseGeniusCollector(ABC):
    def __init__(self, chunk_size: int, max_chunks_number: int, session: Optional[ClientSession] = None):
        self._chunk_size = chunk_size
        self._max_chunks_number = max_chunks_number
        self._session = session
        self._chunks_generator = DataChunksGenerator(chunk_size)

    async def _collect_multiple_chunks(self, chunks: Generator[list, None, None]) -> None:
        for chunk_number, chunk in enumerate(chunks):
            if chunk_number + 1 < self._max_chunks_number:
                await self._collect_single_chunk(chunk)
            else:
                break

    @abstractmethod
    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        raise NotImplementedError

    @staticmethod
    def _is_valid_response(response: dict) -> bool:
        return response.get(META, {}).get(STATUS) == 200

    async def __aenter__(self) -> 'BaseGeniusCollector':
        bearer_token = os.environ[GENIUS_CLIENT_ACCESS_TOKEN]
        headers = {
            "Accept": "application/json",
            "User-Agent": "CompuServe Classic/1.22",
            "Host": "api.genius.com",
            "Authorization": f"Bearer {bearer_token}"
        }
        self._session = await ClientSession(headers=headers).__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.__aexit__(exc_type, exc_val, exc_tb)
