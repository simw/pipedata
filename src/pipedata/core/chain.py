from __future__ import annotations

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

from .optypes import ChainLink, batched_op, filter_op, one2one_op

TStart = TypeVar("TStart")
TEnd = TypeVar("TEnd")
TOther = TypeVar("TOther")


def _identity(input_iterator: Iterator[TEnd]) -> Iterator[TEnd]:
    yield from input_iterator


class ChainType(Generic[TStart, TEnd]):
    @overload
    def __init__(
        self,
        previous_steps: ChainType[TStart, TOther],
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
        previous_steps: Optional[ChainType[TStart, TOther]],
        func: Union[
            Callable[[Iterator[TOther]], Iterator[TEnd]],
            Callable[[Iterator[TStart]], Iterator[TEnd]],
        ],
    ) -> None:
        self._previous_steps = previous_steps
        self._func = ChainLink(func)

    def __call__(self, input_iterator: Iterator[TStart]) -> Iterator[TEnd]:
        if self._previous_steps is None:
            func = cast(ChainLink[TStart, TEnd], self._func)
            return func(input_iterator)

        return self._func(self._previous_steps(input_iterator))  # type: ignore

    def flat_map(
        self, func: Callable[[Iterator[TEnd]], Iterator[TOther]]
    ) -> ChainType[TStart, TOther]:
        """
        Output zero or more elements from one or more input elements.

        This is a fully general operation, that can arbitrarily transform the
        stream of elements. It is the most powerful operation, and all the
        other operations are implemented in terms of it.
        """
        return self.then(func)

    def then(
        self, func: Callable[[Iterator[TEnd]], Iterator[TOther]]
    ) -> ChainType[TStart, TOther]:
        return ChainType(self, func)

    def __or__(
        self, func: Callable[[Iterator[TEnd]], Iterator[TOther]]
    ) -> ChainType[TStart, TOther]:
        return self.then(func)

    def filter(  # noqa: A003
        self, func: Callable[[TEnd], bool]
    ) -> ChainType[TStart, TEnd]:
        """
        Remove elements from the stream that do not pass the filter function.
        """
        return self.then(filter_op(func))

    def map(  # noqa: A003
        self, func: Callable[[TEnd], TOther]
    ) -> ChainType[TStart, TOther]:
        """
        Return a single transformed element from each input element.
        """
        return self.then(one2one_op(func))

    def batched_map(
        self, func: Callable[[Tuple[TEnd, ...]], TOther], n: Optional[int] = None
    ) -> ChainType[TStart, TOther]:
        """
        Return a single transformed element from (up to) n input elements.

        If n is None, then apply the function to all the elements, and return
        an iterator of 1 element.
        """
        return self.then(batched_op(func, n))

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


class Chain(ChainType[TOther, TOther]):
    def __init__(self) -> None:
        super().__init__(None, _identity)
