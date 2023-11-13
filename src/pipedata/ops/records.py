import csv
import io
import logging
from typing import IO, Any, Callable, Dict, Iterator, Optional

import ijson  # type: ignore

logger = logging.getLogger(__name__)


def json_records(
    json_path: str = "item", multiple_values: Optional[bool] = False
) -> Callable[[Iterator[IO[bytes]]], Iterator[Dict[str, Any]]]:
    logger.info(f"Initializing json reader for {json_path}")

    def json_records_func(json_files: Iterator[IO[bytes]]) -> Iterator[Dict[str, Any]]:
        for json_file in json_files:
            logger.info(f"Reading json file {json_file}")
            records = ijson.items(json_file, json_path, multiple_values=multiple_values)
            yield from records

    return json_records_func


def csv_records() -> Callable[[Iterator[IO[bytes]]], Iterator[Dict[str, Any]]]:
    logger.info("Initializing csv reader")

    def csv_records_func(csv_paths: Iterator[IO[bytes]]) -> Iterator[Dict[str, Any]]:
        for csv_path in csv_paths:
            logger.info(f"Reading csv file {csv_path}")
            csv_reader = csv.DictReader(
                io.TextIOWrapper(csv_path, "utf-8"), delimiter=","
            )
            yield from csv_reader

    return csv_records_func
