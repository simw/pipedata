from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from pipedata.core.itertools import take_next, take_up_to_n

TStart = TypeVar("TStart")
TEnd = TypeVar("TEnd")
TOther = TypeVar("TOther")


def _identity(input_iterator: Iterator[TEnd]) -> Iterator[TEnd]:
    while (element := take_next(input_iterator)) is not None:
        yield element


class CountingIterator(Iterator[TStart]):
    def __init__(self, iterator: Iterator[TStart]) -> None:
        self._iterator = iterator
        self._count = 0

    def __iter__(self) -> Iterator[TStart]:
        return self

    def __next__(self) -> TStart:
        self._count += 1
        try:
            return next(self._iterator)
        except StopIteration as err:
            self._count -= 1
            raise StopIteration from err

    def get_count(self) -> int:
        return self._count


class CountedFunc(Generic[TStart, TEnd]):
    def __init__(
        self,
        func: Callable[[Iterator[TStart]], Iterator[TEnd]],
    ) -> None:
        self._func = func
        self._counting_input: Optional[CountingIterator[TStart]] = None
        self._counting_output: Optional[CountingIterator[TEnd]] = None

    @property
    def __name__(self) -> str:  # noqa: A003
        return self._func.__name__

    def __call__(self, input_iterator: Iterator[TStart]) -> Iterator[TEnd]:
        self._counting_input = CountingIterator(input_iterator)
        self._counting_output = CountingIterator(self._func(self._counting_input))
        return self._counting_output

    def get_counts(self) -> Tuple[int, int]:
        return (
            0 if self._counting_input is None else self._counting_input.get_count(),
            0 if self._counting_output is None else self._counting_output.get_count(),
        )


@dataclass
class StepCount:
    name: str
    inputs: int
    outputs: int


class Chain(Generic[TStart, TEnd]):
    @overload
    def __init__(
        self,
        previous_steps: Chain[TStart, TOther],
        func: Callable[[Iterator[TOther]], Iterator[TEnd]],
    ):
        ...

    @overload
    def __init__(
        self,
        previous_steps: None,
        func: Callable[[Iterator[TStart]], Iterator[TEnd]],
    ):
        ...

    def __init__(
        self,
        previous_steps: Optional[Chain[TStart, TOther]],
        func: Union[
            Callable[[Iterator[TOther]], Iterator[TEnd]],
            Callable[[Iterator[TStart]], Iterator[TEnd]],
        ],
    ) -> None:
        self._previous_steps = previous_steps
        self._func = CountedFunc(func)

    def __call__(self, input_iterator: Iterator[TStart]) -> Iterator[TEnd]:
        if self._previous_steps is None:
            func = cast(CountedFunc[TStart, TEnd], self._func)
            return func(input_iterator)

        return self._func(self._previous_steps(input_iterator))  # type: ignore

    def flat_map(
        self, func: Callable[[Iterator[TEnd]], Iterator[TOther]]
    ) -> Chain[TStart, TOther]:
        """
        Output zero or more elements from one or more input elements.

        This is a fully general operation, that can arbitrarily transform the
        stream of elements. It is the most powerful operation, and all the
        other operations are implemented in terms of it.
        """
        return Chain(self, func)

    def filter(self, func: Callable[[TEnd], bool]) -> Chain[TStart, TEnd]:  # noqa: A003
        """
        Remove elements from the stream that do not pass the filter function.
        """

        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TEnd]:
            while (element := take_next(previous_step)) is not None:
                if func(element) is True:
                    yield element

        new_action.__name__ = func.__name__
        return self.flat_map(new_action)

    def map(  # noqa: A003
        self, func: Callable[[TEnd], TOther]
    ) -> Chain[TStart, TOther]:
        """
        Return a single transformed element from each input element.
        """

        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TOther]:
            while (element := take_next(previous_step)) is not None:
                yield func(element)

        new_action.__name__ = func.__name__
        return self.flat_map(new_action)

    def map_tuple(
        self, func: Callable[[Tuple[TEnd, ...]], TOther], n: Optional[int] = None
    ) -> Chain[TStart, TOther]:
        """
        Return a single transformed element from (up to) n input elements.

        If n is None, then apply the function to all the elements, and return
        an iterator of 1 element.
        """

        def new_action(previous_step: Iterator[TEnd]) -> Iterator[TOther]:
            while elements := take_up_to_n(previous_step, n):
                yield func(elements)

        new_action.__name__ = func.__name__
        return self.flat_map(new_action)

    def get_counts(self) -> List[Dict[str, Any]]:
        step_counts = []
        if self._previous_steps is not None:
            step_counts = self._previous_steps.get_counts()

        inputs, outputs = self._func.get_counts()
        step_counts.append(
            {
                "name": self._func.__name__,
                "inputs": inputs,
                "outputs": outputs,
            }
        )
        return step_counts


class ChainStart(Chain[TOther, TOther]):
    def __init__(self) -> None:
        super().__init__(None, _identity)
