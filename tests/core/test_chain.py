from typing import Iterator, Tuple

from pipedata.core import Chain, ChainType


def test_chain() -> None:
    """
    Note: it's important to use a 'falsey' type in the chain as well,
    to check that terminations are explicitly checking for None rather
    than a false value.
    """
    chain = Chain[int]()
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [0, 1, 2, 3]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 4,
            "outputs": 4,
        },
    ]


def test_chain_with_wrong_types() -> None:
    """This test runs fine, but would fail
    mypy due to the incompatible types of the chain.

    Hence, the type: ignores are added.
    TODO: design tests that check that mypy would complain.
    """

    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[str]().filter(is_even)  # type: ignore
    result = list(chain(iter([1, 2, 3])))  # type: ignore
    assert result == [2]  # type: ignore


def test_chain_filter() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]().filter(is_even)

    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [0, 2]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 4,
            "outputs": 4,
        },
        {
            "name": "is_even",
            "inputs": 4,
            "outputs": 2,
        },
    ]

    result2 = list(chain(iter([2, 3, 4])))
    assert result2 == [2, 4]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 3,
            "outputs": 3,
        },
        {
            "name": "is_even",
            "inputs": 3,
            "outputs": 2,
        },
    ]


def test_chain_filter_with_none_passing() -> None:
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]().filter(is_even)
    result = list(chain(iter([1, 3, 5])))
    assert result == []


def test_chain_flat_map() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    chain = Chain[int]().flat_map(add_one)

    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [1, 2, 3, 4]

    result2 = list(chain(iter([2, 3, 4])))
    assert result2 == [3, 4, 5]


def test_chain_then() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    chain = Chain[int]().then(add_one)

    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [1, 2, 3, 4]

    result2 = list(chain(iter([2, 3, 4])))
    assert result2 == [3, 4, 5]


def test_chain_pipe() -> None:
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    chain = Chain[int]() | add_one

    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [1, 2, 3, 4]

    result2 = list(chain(iter([2, 3, 4])))
    assert result2 == [3, 4, 5]


def test_chain_piping_multiple_operations() -> None:  # noqa: C901
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element * 2

    def is_even(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            if element % 2 == 0:
                yield element

    chain = Chain[int]() | add_one | is_even | multiply_two
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [4, 8]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 4,
            "outputs": 4,
        },
        {
            "name": "add_one",
            "inputs": 4,
            "outputs": 4,
        },
        {
            "name": "is_even",
            "inputs": 4,
            "outputs": 2,
        },
        {
            "name": "multiply_two",
            "inputs": 2,
            "outputs": 2,
        },
    ]


def test_chain_multiple_operations() -> None:  # noqa: C901
    def add_one(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element + 1

    def multiply_two(input_iterator: Iterator[int]) -> Iterator[int]:
        for element in input_iterator:
            yield element * 2

    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]().then(add_one).filter(is_even).then(multiply_two)
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [4, 8]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 4,
            "outputs": 4,
        },
        {
            "name": "add_one",
            "inputs": 4,
            "outputs": 4,
        },
        {
            "name": "is_even",
            "inputs": 4,
            "outputs": 2,
        },
        {
            "name": "multiply_two",
            "inputs": 2,
            "outputs": 2,
        },
    ]


def test_chain_map() -> None:
    def add_one(value: int) -> int:
        return value + 1

    chain = Chain[int]().map(add_one)
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [1, 2, 3, 4]


def test_chain_map_changing_types() -> None:
    chain: ChainType[int, str] = Chain[int]().map(str)
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == ["0", "1", "2", "3"]


def test_chain_batched_map() -> None:
    def add_values(values: Tuple[int, ...]) -> int:
        return sum(values)

    chain = Chain[int]().batched_map(add_values, 2)
    result = list(chain(iter([0, 1, 2, 3, 4])))
    assert result == [1, 5, 4]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 5,
            "outputs": 5,
        },
        {
            "name": "add_values",
            "inputs": 5,
            "outputs": 3,
        },
    ]
