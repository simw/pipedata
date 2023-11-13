import csv
import io
import logging
from typing import IO, Any, Callable, Dict, Iterator, Optional, Union

import ijson  # type: ignore

from .files import OpenedFileRef

logger = logging.getLogger(__name__)


def json_records(
    json_path: str = "item", multiple_values: Optional[bool] = False
) -> Callable[[Iterator[Union[IO[bytes], OpenedFileRef]]], Iterator[Dict[str, Any]]]:
    logger.info(f"Initializing json reader for {json_path}")

    def json_records_func(
        json_files: Iterator[Union[IO[bytes], OpenedFileRef]]
    ) -> Iterator[Dict[str, Any]]:
        for json_file in json_files:
            if isinstance(json_file, OpenedFileRef):
                contents = json_file.contents
                logger.info(f"Reading json file {json_file.name}")
            else:
                contents = json_file
                logger.info(f"Reading json file {json_file}")
            records = ijson.items(contents, json_path, multiple_values=multiple_values)
            yield from records

    return json_records_func


def csv_records() -> (
    Callable[[Iterator[Union[IO[bytes], OpenedFileRef]]], Iterator[Dict[str, Any]]]
):
    logger.info("Initializing csv reader")

    def csv_records_func(
        csv_files: Iterator[Union[IO[bytes], OpenedFileRef]]
    ) -> Iterator[Dict[str, Any]]:
        for csv_file in csv_files:
            if isinstance(csv_file, OpenedFileRef):
                contents = csv_file.contents
                logger.info(f"Reading csv file {csv_file.name}")
            else:
                contents = csv_file
                logger.info(f"Reading csv file {csv_file}")
            csv_reader = csv.DictReader(
                io.TextIOWrapper(contents, "utf-8"), delimiter=","
            )
            yield from csv_reader

    return csv_records_func
