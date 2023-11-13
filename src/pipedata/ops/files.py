import logging
import zipfile
from typing import IO, Iterator

import fsspec  # type: ignore

logger = logging.getLogger(__name__)


def zipped_files(file_refs: Iterator[str]) -> Iterator[IO[bytes]]:
    logger.info(f"Initializing zipped files reader")
    for file_ref in file_refs:
        logger.info(f"Opening zip file at {file_ref}")
        with fsspec.open(file_ref, "rb") as file:
            with zipfile.ZipFile(file) as zip_file:
                names = zip_file.namelist()
                logger.info(f"Found {len(names)} files in zip file")
                for i, name in enumerate(names):
                    logger.info(f"Reading file {i} ({name}) from zip file")
                    with zip_file.open(name) as inner_file:
                        yield inner_file
