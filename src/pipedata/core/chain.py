from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from .links import ChainLink

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

    def then(
        self, func: Callable[[Iterator[TEnd]], Iterator[TOther]]
    ) -> ChainType[TStart, TOther]:
        return ChainType(self, func)

    def __or__(
        self, func: Callable[[Iterator[TEnd]], Iterator[TOther]]
    ) -> ChainType[TStart, TOther]:
        return self.then(func)

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
