from typing import Iterator

from pipedata.core import start_stream
from pipedata.core.itertools import take_next, take_up_to_n


def test_stream_to_list() -> None:
    result = start_stream([1, 2, 3]).to_list()
    assert result == [1, 2, 3]


def test_stream_to_list_smaller_length() -> None:
    result = start_stream([1, 2, 3]).to_list(2)
    assert result == [1, 2]


def test_stream_to_list_longer_length() -> None:
    result = start_stream([1, 2, 3]).to_list(4)
    assert result == [1, 2, 3]


def test_filter_is_even() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = start_stream([1, 2, 3, 4, 5]).filter(is_even).to_list()
    assert result == [2, 4]


def test_filter_with_none_passing() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = start_stream([1, 3, 5]).filter(is_even).to_list()
    assert result == []


def test_flat_map_identity_stream() -> None:
    def identity(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element

    result = start_stream([1, 2, 3]).flat_map(identity).to_list()
    assert result == [1, 2, 3]


def test_flat_map_stream_chain() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element * 2

    result = start_stream([1, 2, 3]).flat_map(add_one).flat_map(multiply_two).to_list()
    assert result == [4, 6, 8]


def test_flat_map_growing_stream() -> None:
    def add_element(input_iterator: Iterator[int]) -> Iterator[int]:
        while element := take_next(input_iterator):
            yield element
            yield element + 1

    result = start_stream([1, 2, 3]).flat_map(add_element).to_list()
    assert result == [1, 2, 2, 3, 3, 4]


def test_flat_map_shrinking_stream() -> None:
    def add_two_values(input_iterator: Iterator[int]) -> Iterator[int]:
        while batch := take_up_to_n(input_iterator, 2):
            yield sum(batch)

    result = start_stream([1, 2, 3]).flat_map(add_two_values).to_list()
    assert result == [3, 3]
