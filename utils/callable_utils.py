import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any


async def run_async(func: Callable, max_workers: int = 1) -> Any:
    with ThreadPoolExecutor(max_workers) as pool:
        return await asyncio.get_event_loop().run_in_executor(pool, func)
