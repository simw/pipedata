from itertools import islice
from typing import Iterator, Optional, Tuple, TypeVar

T = TypeVar("T")


def take_next(iterator: Iterator[T]) -> Optional[T]:
    try:
        return next(iterator)
    except StopIteration:
        return None


def take_up_to_n(iterator: Iterator[T], n: int) -> Tuple[T, ...]:
    return tuple(islice(iterator, n))
