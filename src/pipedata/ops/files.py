import logging
import zipfile
from dataclasses import dataclass
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
)

import fsspec  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.dataset as pa_dataset  # type: ignore

logger = logging.getLogger(__name__)


class FilesReaderError(Exception):
    pass


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


def read_from_parquet(
    columns: Optional[Union[List[str], Dict[str, Any]]] = None,
    return_as: Literal["recordbatch", "record"] = "record",
    batch_size: Optional[int] = 100_000,
) -> Callable[[Iterator[str]], Iterator[Union[Dict[str, Any], pa.RecordBatch]]]:
    logger.info(f"Initializing parquet reader with {batch_size=}")

    if return_as not in ("recordbatch", "record"):
        raise FilesReaderError(f"Unknown return_as value {return_as}")

    def parquet_batch_reader(
        file_refs: Iterator[str],
    ) -> Iterator[Union[Dict[str, Any], pa.RecordBatch]]:
        for file_ref in file_refs:
            logger.info(f"Reading parquet file {file_ref}")
            ds = pa_dataset.dataset(file_ref, format="parquet")
            for batch in ds.to_batches(columns=columns, batch_size=batch_size):
                if return_as == "recordbatch":
                    yield batch
                elif return_as == "record":
                    yield from batch.to_pylist()
                else:
                    raise FilesReaderError(
                        f"Unknown return_as value {return_as}"
                    )  # pragma: no cover

    return parquet_batch_reader
