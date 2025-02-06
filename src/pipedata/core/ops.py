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


class batched(ChainLink[TEnd, TOther]):  # noqa: N801
    def __init__(self, func: Callable[[Tuple[TEnd, ...]], TOther], n: Optional[int]):
        @functools.wraps(func)
        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TOther]:
            return (func(elements) for elements in _batched(previous_step, n))

        super().__init__(new_action)


class chain_iterables(ChainLink[Iterator[TEnd], TEnd]):  # noqa: N801
    def __init__(self) -> None:
        def chain_iterables_(previous_step: Iterator[Iterator[TEnd]]) -> Iterator[TEnd]:
            return itertools.chain.from_iterable(previous_step)

        super().__init__(chain_iterables_)


class grouper(ChainLink[TEnd, list[TEnd]]):  # noqa: N801
    def __init__(
        self,
        *,
        starter: Optional[Callable[[TEnd], bool]] = None,
        ender: Optional[Callable[[TEnd], bool]] = None,
    ) -> None:
        def grouper_(previous_step: Iterator[TEnd]) -> Iterator[list[TEnd]]:
            group: list[TEnd] = []
            for element in previous_step:
                if starter is not None and starter(element) and len(group) > 0:
                    yield group
                    group = [element]
                elif ender is not None and ender(element):
                    group.append(element)
                    yield group
                    group = []
                else:
                    group.append(element)

            if len(group) > 0:
                yield group

        super().__init__(grouper_)
