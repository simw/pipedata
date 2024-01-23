import functools
import itertools
from typing import (
    Callable,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
)

from .links import ChainLink

TStart = TypeVar("TStart")
TEnd = TypeVar("TEnd")
TOther = TypeVar("TOther")


def _batched(iterable: Iterator[TEnd], n: Optional[int]) -> Iterator[Tuple[TEnd, ...]]:
    """Can be replaced by itertools.batched once using Python 3.12+."""
    while (elements := tuple(itertools.islice(iterable, n))) != ():
        yield elements


class filtering(ChainLink[TEnd, TEnd]):  # noqa: N801
    def __init__(self, func: Callable[[TEnd], bool]):
        @functools.wraps(func)
        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TEnd]:
            return filter(func, previous_step)

        super().__init__(new_action)


class mapping(ChainLink[TEnd, TOther]):  # noqa: N801
    def __init__(self, func: Callable[[TEnd], TOther]):
        @functools.wraps(func)
        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TOther]:
            return map(func, previous_step)

        super().__init__(new_action)


class batching(ChainLink[TEnd, TOther]):  # noqa: N801
    def __init__(self, func: Callable[[Tuple[TEnd, ...]], TOther], n: Optional[int]):
        @functools.wraps(func)
        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TOther]:
            return (func(elements) for elements in _batched(previous_step, n))

        super().__init__(new_action)
