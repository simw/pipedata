from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
)

TStart = TypeVar("TStart")
TEnd = TypeVar("TEnd")
TOther = TypeVar("TOther")


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


class ChainLink(Generic[TStart, TEnd]):
    def __init__(
        self,
        func: Callable[[Iterator[TStart]], Iterator[TEnd]],
    ) -> None:
        self._func = func
        self._input: Optional[CountingIterator[TStart]] = None
        self._output: Optional[CountingIterator[TEnd]] = None

    @property
    def __name__(self) -> str:  # noqa: A003
        return self._func.__name__

    def __call__(self, input_iterator: Iterator[TStart]) -> Iterator[TEnd]:
        self._input = CountingIterator(input_iterator)
        self._output = CountingIterator(self._func(self._input))
        return self._output

    def get_counts(self) -> Tuple[int, int]:
        return (
            0 if self._input is None else self._input.get_count(),
            0 if self._output is None else self._output.get_count(),
        )
