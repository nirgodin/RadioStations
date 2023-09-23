from abc import abstractmethod, ABC
from typing import Optional, Generator, List

from aiohttp import ClientSession

from consts.genius_consts import META, STATUS
from tools.data_chunks_generator import DataChunksGenerator
from utils.genius_utils import build_genius_headers


class BaseGeniusCollector(ABC):
    def __init__(self, chunk_size: int, max_chunks_number: int, session: Optional[ClientSession] = None):
        self._chunk_size = chunk_size
        self._max_chunks_number = max_chunks_number
        self._session = session
        self._chunks_generator = DataChunksGenerator(chunk_size, max_chunks_number)

    @abstractmethod
    async def collect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        raise NotImplementedError

    @staticmethod
    def _is_valid_response(response: dict) -> bool:
        return response.get(META, {}).get(STATUS) == 200

    async def __aenter__(self) -> 'BaseGeniusCollector':
        headers = build_genius_headers()
        self._session = await ClientSession(headers=headers).__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.__aexit__(exc_type, exc_val, exc_tb)
