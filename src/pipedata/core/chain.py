from __future__ import annotations

from typing import Callable, Generic, Iterator, Optional, Tuple, TypeVar

from pipedata.core.itertools import take_next, take_up_to_n

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

    def flat_map(
        self, func: Callable[[Iterator[TEnd]], Iterator[TNewEnd]]
    ) -> Chain[TStart, TNewEnd]:
        """
        Output zero or more elements from one or more input elements.

        This is a fully general operation, that can arbitrarily transform the
        stream of elements. It is the most powerful operation, and all the
        other operations are implemented in terms of it.
        """

        def new_action(previous_step: Iterator[TStart]) -> Iterator[TNewEnd]:
            return func(self._action(previous_step))

        return Chain(new_action)

    def filter(self, func: Callable[[TEnd], bool]) -> Chain[TStart, TEnd]:  # noqa: A003
        """
        Remove elements from the stream that do not pass the filter function.
        """

        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TEnd]:
            while element := take_next(previous_step):
                if func(element) is True:
                    yield element

        return self.flat_map(new_action)

    def map(  # noqa: A003
        self, func: Callable[[TEnd], TNewEnd]
    ) -> Chain[TStart, TNewEnd]:
        """
        Return a single transformed element from each input element.
        """

        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TNewEnd]:
            while element := take_next(previous_step):
                yield func(element)

        return self.flat_map(new_action)

    def map_tuple(
        self, func: Callable[[Tuple[TEnd, ...]], TNewEnd], n: Optional[int] = None
    ) -> Chain[TStart, TNewEnd]:
        """
        Return a single transformed element from (up to) n input elements.

        If n is None, then apply the function to all the elements, and return
        an iterator of 1 element.
        """

        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TNewEnd]:
            while elements := take_up_to_n(previous_step, n):
                yield func(elements)

        return self.flat_map(new_action)
