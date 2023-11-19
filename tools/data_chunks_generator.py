from functools import partial
from inspect import iscoroutinefunction
from math import ceil
from typing import Generator, Optional, Union, Any, Type

from asyncio_pool import AioPool
from tqdm import tqdm

from consts.api_consts import AIO_POOL_SIZE
from consts.typing_consts import AF, F


class DataChunksGenerator:
    def __init__(self, chunk_size: int = 50, max_chunks_number: Optional[int] = 5):
        self._chunk_size = chunk_size
        self._max_chunks_number = max_chunks_number

    async def execute_by_chunk_in_parallel(self,
                                           lst: list,
                                           filtering_list: Optional[list],
                                           element_func: AF,
                                           chunk_func: F,
                                           expected_type: Type[Any]):
        chunks = self.generate_data_chunks(lst=lst, filtering_list=filtering_list)

        for chunk in chunks:
            results = await self._execute_single_chunk(element_func, chunk)
            valid_results = [res for res in results if isinstance(res, expected_type)]
            chunk_func(valid_results)

    async def execute_by_chunk(self, lst: list, filtering_list: Optional[list], func: Union[F, AF]) -> None:
        chunks = self.generate_data_chunks(lst, filtering_list)

        for i, chunk in enumerate(chunks):
            if iscoroutinefunction(func):
                await func(chunk)
            else:
                func(chunk)

    def generate_data_chunks(self, lst: list, filtering_list: Optional[list]) -> Generator[list, None, None]:
        if filtering_list is not None:
            lst = [artist for artist in lst if artist not in filtering_list]

        total_chunks = ceil(len(lst) / self._chunk_size)
        n_chunks = total_chunks if self._max_chunks_number is None else min(total_chunks, self._max_chunks_number)
        current_chunk = 0

        for i in range(0, len(lst), self._chunk_size):
            if current_chunk == n_chunks:
                break

            print(f'Generating chunk {self._get_chunk_number(i)} out of {n_chunks} (Total: {total_chunks})')
            yield lst[i: i + self._chunk_size]
            current_chunk += 1

    async def _execute_single_chunk(self, func: AF, chunk: list) -> Any:
        pool = AioPool(AIO_POOL_SIZE)

        with tqdm(total=len(chunk)) as progress_bar:
            func = partial(self._execute_single_element, progress_bar, func)
            return await pool.map(func, chunk)

    @staticmethod
    async def _execute_single_element(progress_bar: tqdm, func: AF, element: Any) -> Any:
        result = await func(element)
        progress_bar.update(1)

        return result

    def _get_chunk_number(self, index: int) -> int:
        chunk_number = (index / self._chunk_size) + 1
        return int(chunk_number)
