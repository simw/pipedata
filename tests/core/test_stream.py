from typing import Iterable, Iterator, List

from pipedata.core import Chain, start_stream
from pipedata.core.itertools import take_next, take_up_to_n


def test_stream_to_list() -> None:
    result = start_stream([1, 2, 3]).to_list()
    assert result == [1, 2, 3]


def test_stream_with_a_chain() -> None:
    chain = Chain[int, int].start().filter(lambda x: x % 2 == 0)

    result = start_stream([1, 2, 3]).flat_map(chain).to_list()
    assert result == [2]


def test_stream_to_list_smaller_length() -> None:
    result = start_stream([1, 2, 3]).to_list(2)
    assert result == [1, 2]


def test_stream_to_list_longer_length() -> None:
    result = start_stream([1, 2, 3]).to_list(4)
    assert result == [1, 2, 3]


def test_stream_filter() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = start_stream([1, 2, 3, 4, 5]).filter(is_even).to_list()
    assert result == [2, 4]


def test_stream_filter_with_none_passing() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = start_stream([1, 3, 5]).filter(is_even).to_list()
    assert result == []


def test_stream_flat_map_identity() -> None:
    def identity(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element

    result = start_stream([1, 2, 3]).flat_map(identity).to_list()
    assert result == [1, 2, 3]


def test_stream_flat_map_chain() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element * 2

    result = start_stream([1, 2, 3]).flat_map(add_one).flat_map(multiply_two).to_list()
    assert result == [4, 6, 8]


def test_stream_flat_map_growing() -> None:
    def add_element(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element
            yield element + 1

    result = start_stream([1, 2, 3]).flat_map(add_element).to_list()
    assert result == [1, 2, 2, 3, 3, 4]


def test_stream_flat_map_shrinking() -> None:
    def add_two_values(input_iterator: Iterator[int]) -> Iterator[int]:
        while batch := take_up_to_n(input_iterator, 2):
            yield sum(batch)

    result = start_stream([1, 2, 3]).flat_map(add_two_values).to_list()
    assert result == [3, 3]


def test_stream_map() -> None:
    def add_one(value: int) -> int:
        return value + 1

    result = start_stream([1, 2, 3]).map(add_one).to_list()
    assert result == [2, 3, 4]


def test_stream_reduce_adding() -> None:
    def add_values(a: int, b: int) -> int:
        return a + b

    result = start_stream([1, 2, 3]).reduce(add_values)
    expected = 6
    assert result == expected


def test_stream_reduce_appending() -> None:
    def append_values(a: List[int], b: int) -> List[int]:
        a.append(b)
        return a

    initializer: List[int] = []
    result = start_stream([1, 2, 3]).reduce(append_values, initializer)
    assert result == [1, 2, 3]


def test_stream_map_tuple() -> None:
    def add_values(values: Iterable[int]) -> int:
        return sum(values)

    result = start_stream([1, 2, 3]).map_tuple(add_values, 2).to_list()
    assert result == [3, 3]
