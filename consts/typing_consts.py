from typing import TypeVar, Callable, Any, Awaitable

AF = TypeVar("AF", bound=Callable[..., Awaitable[Any]])
F = TypeVar("F", bound=Callable[..., Any])
