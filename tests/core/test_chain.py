from typing import Iterator, Tuple

from pipedata.core import Chain, ChainType, ops


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

    chain = Chain[str]().then(ops.filtering(is_even))  # type: ignore
    result = list(chain(iter([1, 2, 3])))  # type: ignore
    assert result == [2]


def test_chain_filter() -> None:
    @ops.filtering
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]().then(is_even)

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
    @ops.filtering
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]().then(is_even)
    result = list(chain(iter([1, 3, 5])))
    assert result == []


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

    @ops.mapping
    def multiply_two(value: int) -> int:
        return value * 2

    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]() | add_one | ops.filtering(is_even) | multiply_two
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

    @ops.filtering
    def is_even(value: int) -> bool:
        return value % 2 == 0

    chain = Chain[int]().then(add_one).then(is_even).then(multiply_two)
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

    chain = Chain[int]().then(ops.mapping(add_one))
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == [1, 2, 3, 4]


def test_chain_map_changing_types() -> None:
    chain: ChainType[int, str] = Chain[int]().then(ops.mapping(str))
    result = list(chain(iter([0, 1, 2, 3])))
    assert result == ["0", "1", "2", "3"]


def test_chain_batched_map() -> None:
    def add_values(values: Tuple[int, ...]) -> int:
        return sum(values)

    chain = Chain[int]().then(ops.batched(add_values, 2))
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


def test_chain_iterables() -> None:
    # TODO: make typing work with chain_iterables
    chain = Chain[int]().then(ops.chain_iterables())  # type: ignore
    inputs = iter([iter([0, 1]), iter([2, 3])])
    result = list(chain(inputs))  # type: ignore
    assert result == [0, 1, 2, 3]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 2,
            "outputs": 2,
        },
        {
            "name": "chain_iterables_",
            "inputs": 2,
            "outputs": 4,
        },
    ]


def test_chain_grouper() -> None:
    def is_one(val: int) -> bool:
        return val == 1

    def is_three(val: int) -> bool:
        return val == 3  # noqa: PLR2004

    chain = Chain[int]().then(ops.grouper(starter=is_one, ender=is_three))
    inputs = [1, 2, 3, 4, 1, 2, 3, 4]
    result = list(chain(iter(inputs)))
    assert result == [[1, 2, 3], [4], [1, 2, 3], [4]]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 8,
            "outputs": 8,
        },
        {
            "name": "grouper_",
            "inputs": 8,
            "outputs": 4,
        },
    ]


def test_chain_grouper_no_end() -> None:
    def is_four(val: int) -> bool:
        return val == 4  # noqa: PLR2004

    chain = Chain[int]().then(ops.grouper(ender=is_four))
    inputs = [1, 2, 3, 4, 1, 2, 3, 4]
    result = list(chain(iter(inputs)))
    assert result == [[1, 2, 3, 4], [1, 2, 3, 4]]
    assert chain.get_counts() == [
        {
            "name": "_identity",
            "inputs": 8,
            "outputs": 8,
        },
        {
            "name": "grouper_",
            "inputs": 8,
            "outputs": 2,
        },
    ]
