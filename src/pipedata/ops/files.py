import logging
import zipfile
from typing import IO, Iterator

import fsspec  # type: ignore

logger = logging.getLogger(__name__)


def zipped_files(file_refs: Iterator[str]) -> Iterator[IO[bytes]]:
    for file_ref in file_refs:
        with fsspec.open(file_ref, "rb") as file:
            with zipfile.ZipFile(file) as zip_file:
                for name in zip_file.namelist():
                    with zip_file.open(name) as inner_file:
                        yield inner_file
