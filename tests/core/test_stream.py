from itertools import islice
from typing import Iterable, Iterator, List

from pipedata.core import Chain, Stream


def test_stream_to_list() -> None:
    result = Stream([1, 2, 3]).to_list()
    assert result == [1, 2, 3]


def test_stream_on_range() -> None:
    stream = Stream(range(3))
    result = stream.to_list()
    assert result == [0, 1, 2]
    assert stream.get_counts() == [
        {"name": "_identity", "inputs": 3, "outputs": 3},
    ]


def test_repeated_stream() -> None:
    stream = Stream([0, 1, 2, 3])
    result1 = stream.to_list()
    assert result1 == [0, 1, 2, 3]

    # Note: the stream is already exhausted
    result2 = stream.to_list()
    assert result2 == []


def test_stream_is_iterable() -> None:
    stream = Stream([0, 1, 2, 3])
    result = list(stream)
    assert result == [0, 1, 2, 3]

    stream2 = Stream([2, 3, 4])
    result2 = []
    for element in stream2:
        result2.append(element)  # noqa: PERF402
    assert result2 == [2, 3, 4]


def test_stream_with_a_chain() -> None:
    chain = Chain[int]().filter(lambda x: x % 2 == 0)

    result = Stream([0, 1, 2, 3]).flat_map(chain).to_list()
    assert result == [0, 2]


def test_stream_to_list_smaller_length() -> None:
    result = Stream([0, 1, 2, 3]).to_list(2)
    assert result == [0, 1]


def test_stream_to_list_longer_length() -> None:
    result = Stream([0, 1, 2, 3]).to_list(5)
    assert result == [0, 1, 2, 3]


def test_stream_filter() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = Stream([0, 1, 2, 3, 4, 5]).filter(is_even).to_list()
    assert result == [0, 2, 4]


def test_stream_filter_with_range() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = Stream(range(10)).filter(is_even).to_list()
    assert result == [0, 2, 4, 6, 8]


def test_stream_filter_with_none_passing() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    result = Stream([1, 3, 5]).filter(is_even).to_list()
    assert result == []


def test_stream_flat_map_identity() -> None:
    def identity(input_iterator: Iterator[int]) -> Iterator[int]:
        yield from input_iterator

    result = Stream([0, 1, 2, 3]).flat_map(identity).to_list()
    assert result == [0, 1, 2, 3]


def test_stream_then_chain() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element * 2

    result = Stream([0, 1, 2, 3]).then(add_one).then(multiply_two).to_list()
    assert result == [2, 4, 6, 8]


def test_stream_pipe_chain() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element * 2

    stream = Stream([0, 1, 2, 3]) | add_one | multiply_two
    result = stream.to_list()
    assert result == [2, 4, 6, 8]


def test_stream_flat_map_growing() -> None:
    def add_element(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element
            yield element + 1

    result = Stream([0, 1, 2, 3]).flat_map(add_element).to_list()
    assert result == [0, 1, 1, 2, 2, 3, 3, 4]


def test_stream_flat_map_shrinking() -> None:
    def add_two_values(input_iterator: Iterator[int]) -> Iterator[int]:
        while batch := tuple(islice(input_iterator, 2)):
            yield sum(batch)

    result = Stream([0, 1, 2, 3, 4]).flat_map(add_two_values).to_list()
    assert result == [1, 5, 4]


def test_stream_map() -> None:
    def add_one(value: int) -> int:
        return value + 1

    result = Stream([0, 1, 2, 3]).map(add_one).to_list()
    assert result == [1, 2, 3, 4]


def test_stream_reduce_adding() -> None:
    def add_values(a: int, b: int) -> int:
        return a + b

    result = Stream([0, 1, 2, 3]).reduce(add_values)
    expected = 6
    assert result == expected


def test_stream_reduce_appending() -> None:
    def append_values(a: List[int], b: int) -> List[int]:
        a.append(b)
        return a

    initializer: List[int] = []
    result = Stream([0, 1, 2, 3]).reduce(append_values, initializer)
    assert result == [0, 1, 2, 3]


def test_stream_batched_map() -> None:
    def add_values(values: Iterable[int]) -> int:
        return sum(values)

    result = Stream([0, 1, 2, 3, 4]).batched_map(add_values, 2).to_list()
    assert result == [1, 5, 4]
