from inspect import iscoroutinefunction
from math import ceil
from typing import Generator, Optional, Union

from consts.typing_consts import AF, F


class DataChunksGenerator:
    def __init__(self, chunk_size: int = 50, max_chunks_number: Optional[int] = 5):
        self._chunk_size = chunk_size
        self._max_chunks_number = max_chunks_number

    async def execute_by_chunk(self, lst: list, filtering_list: Optional[list], func: Union[F, AF]) -> None:
        chunks = self.generate_data_chunks(lst, filtering_list)

        for i, chunk in enumerate(chunks):
            if i + 1 == self._max_chunks_number:
                break
            elif iscoroutinefunction(func):
                await func(chunk)
            else:
                func(chunk)

    def generate_data_chunks(self, lst: list, filtering_list: Optional[list]) -> Generator[list, None, None]:
        if filtering_list is not None:
            lst = [artist for artist in lst if artist not in filtering_list]

        total_chunks = ceil(len(lst) / self._chunk_size)
        n_chunks = min(total_chunks, self._max_chunks_number)

        for i in range(0, len(lst), self._chunk_size):
            print(f'Generating chunk {self._get_chunk_number(i)} out of {n_chunks} (Total: {total_chunks})')
            yield lst[i: i + self._chunk_size]

    def _get_chunk_number(self, index: int) -> int:
        chunk_number = (index / self._chunk_size) + 1
        return int(chunk_number)
