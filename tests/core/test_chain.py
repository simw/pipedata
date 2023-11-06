from typing import Iterator

from pipedata.core import Chain
from pipedata.core.itertools import take_next


def test_chain() -> None:
    chain = Chain[int, int].start()
    result = list(chain(iter([1, 2, 3])))
    assert result == [1, 2, 3]


def test_chain_filter() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int, int].start().filter(is_even)

    result = list(chain(iter([1, 2, 3])))
    assert result == [2]

    result2 = list(chain(iter([2, 3, 4])))
    assert result2 == [2, 4]


def test_chain_filter_with_none_passing() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int, int].start().filter(is_even)
    result = list(chain(iter([1, 3, 5])))
    assert result == []


def test_chain_flat_map() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element + 1

    chain = Chain[int, int].start().flat_map(add_one)

    result = list(chain(iter([1, 2, 3])))
    assert result == [2, 3, 4]

    result2 = list(chain(iter([2, 3, 4])))
    assert result2 == [3, 4, 5]


def test_chain_multiple_operations() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element * 2

    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = (
        Chain[int, int].start().flat_map(add_one).filter(is_even).flat_map(multiply_two)
    )
    result = list(chain(iter([1, 2, 3])))
    assert result == [4, 8]
