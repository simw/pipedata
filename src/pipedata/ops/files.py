import logging
import zipfile
from dataclasses import dataclass
from typing import IO, Iterator

import fsspec  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class OpenedFileRef:
    name: str
    contents: IO[bytes]


def zipped_files(file_refs: Iterator[str]) -> Iterator[OpenedFileRef]:
    logger.info("Initializing zipped files reader")
    for file_ref in file_refs:
        logger.info(f"Opening zip file at {file_ref}")
        with fsspec.open(file_ref, "rb") as file:
            with zipfile.ZipFile(file) as zip_file:
                infos = zip_file.infolist()
                logger.info(f"Found {len(infos)} files in zip file")
                for i, info in enumerate(infos):
                    name = info.filename
                    logger.info(f"Reading file {i} ({name}) from zip file")
                    with zip_file.open(name) as inner_file:
                        yield OpenedFileRef(
                            name=name,
                            contents=inner_file,
                        )
