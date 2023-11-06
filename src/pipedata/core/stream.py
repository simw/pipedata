from __future__ import annotations

import itertools
from typing import Callable, Iterable, Iterator, List, Optional, TypeVar

from .chain import Chain

TStart = TypeVar("TStart")
TEnd = TypeVar("TEnd")
TNewEnd = TypeVar("TNewEnd")


class Stream(Iterable[TEnd]):
    def __init__(self, items: Iterable[TStart], chain: Chain[TStart, TEnd]) -> None:
        self._items = iter(items)
        self._chain = chain
        self._iter = self._chain(self._items)

    def __iter__(self) -> Iterator[TEnd]:
        return self

    def __next__(self) -> TEnd:
        return next(self._iter)

    def filter(self, func: Callable[[TEnd], bool]) -> Stream[TEnd]:  # noqa: A003
        return Stream(self._items, self._chain.filter(func))

    def flat_map(
        self, func: Callable[[Iterator[TEnd]], Iterator[TNewEnd]]
    ) -> Stream[TNewEnd]:
        return Stream(self._items, self._chain.flat_map(func))

    def to_list(
        self,
        stop: Optional[int] = None,
    ) -> List[TEnd]:
        return list(itertools.islice(self, stop))


def start_stream(items: Iterable[TStart]) -> Stream[TStart]:
    chain = Chain[TStart, TStart].start()
    return Stream(items, chain)
