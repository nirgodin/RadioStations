from typing import Generator, Optional


class DataChunksGenerator:
    def __init__(self, chunk_size: int = 50):
        self._chunk_size = chunk_size

    def generate_data_chunks(self, lst: list, filtering_list: Optional[list]) -> Generator[list, None, None]:
        if filtering_list is not None:
            lst = [artist for artist in lst if artist not in filtering_list]

        n_chunks = round(len(lst) / self._chunk_size)

        for i in range(0, len(lst), self._chunk_size):
            print(f'Generating chunk {self._get_chunk_number(i)} out of {n_chunks}')
            yield lst[i: i + self._chunk_size]

    def _get_chunk_number(self, index: int) -> int:
        chunk_number = (index / self._chunk_size) + 1
        return int(chunk_number)
