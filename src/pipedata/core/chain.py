from __future__ import annotations

from typing import Callable, Generic, Iterator, TypeVar

from pipedata.core.itertools import take_next

TStart = TypeVar("TStart")
TEnd = TypeVar("TEnd")
TNewEnd = TypeVar("TNewEnd")


def _identity(input_iterator: Iterator[TEnd]) -> Iterator[TEnd]:
    while element := take_next(input_iterator):
        yield element


class Chain(Generic[TStart, TEnd]):
    def __init__(self, action: Callable[[Iterator[TStart]], Iterator[TEnd]]) -> None:
        self._action = action

    @classmethod
    def start(cls) -> Chain[TEnd, TEnd]:
        return Chain(_identity)

    def __call__(self, previous_step: Iterator[TStart]) -> Iterator[TEnd]:
        return self._action(previous_step)

    def filter(self, func: Callable[[TEnd], bool]) -> Chain[TStart, TEnd]:  # noqa: A003
        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TEnd]:
            while element := take_next(previous_step):
                if func(element) is True:
                    yield element

        return self.flat_map(new_action)

    def flat_map(
        self, func: Callable[[Iterator[TEnd]], Iterator[TNewEnd]]
    ) -> Chain[TStart, TNewEnd]:
        def new_action(previous_step: Iterator[TStart]) -> Iterator[TNewEnd]:
            return func(self._action(previous_step))

        return Chain(new_action)
