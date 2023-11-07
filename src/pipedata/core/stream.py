from __future__ import annotations

import functools
import itertools
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    overload,
)

from .chain import Chain, ChainStart

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

    def flat_map(
        self, func: Callable[[Iterator[TEnd]], Iterator[TNewEnd]]
    ) -> Stream[TNewEnd]:
        return Stream(self._items, self._chain.flat_map(func))

    def filter(self, func: Callable[[TEnd], bool]) -> Stream[TEnd]:  # noqa: A003
        return Stream(self._items, self._chain.filter(func))

    def map(self, func: Callable[[TEnd], TNewEnd]) -> Stream[TNewEnd]:  # noqa: A003
        return Stream(self._items, self._chain.map(func))

    def batched_map(
        self, func: Callable[[Tuple[TEnd, ...]], TNewEnd], n: Optional[int] = None
    ) -> Stream[TNewEnd]:
        return Stream(self._items, self._chain.batched_map(func, n))

    @overload
    def reduce(self, func: Callable[[TEnd, TEnd], TEnd]) -> TEnd:
        ...

    @overload
    def reduce(
        self, func: Callable[[TNewEnd, TEnd], TNewEnd], initializer: TNewEnd
    ) -> TNewEnd:
        ...

    def reduce(self, func, initializer=None):  # type: ignore
        if initializer is None:
            return functools.reduce(func, self)
        return functools.reduce(func, self, initializer)

    def to_list(
        self,
        stop: Optional[int] = None,
    ) -> List[TEnd]:
        return list(itertools.islice(self, stop))

    def get_counts(self) -> List[Dict[str, Any]]:
        return self._chain.get_counts()


class StreamStart(Stream[TEnd]):
    def __init__(self, items: Iterable[TEnd]) -> None:
        super().__init__(items, ChainStart[TEnd]())
