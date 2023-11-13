import logging
from typing import Any, Callable, Dict, Iterator, Optional

import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from pipedata.core.chain import batched

logger = logging.getLogger(__name__)


def parquet_writer(
    file_path: str,
    schema: Optional[pa.Schema] = None,
    row_group_length: Optional[int] = None,
    max_file_length: Optional[int] = None,
) -> Callable[[Iterator[Dict[str, Any]]], Iterator[str]]:
    if row_group_length is None and max_file_length is not None:
        row_group_length = max_file_length

    multi_file = max_file_length is not None
    if multi_file:
        if file_path.format(i=1) == file_path:
            msg = "When (possibly) writing to multiple files (as the file_length"
            msg += " argument is not None), the file_path argument must be a"
            msg += " format string that contains a format specifier for the file."
            raise ValueError(msg)

    logger.info(f"Initializing parquet writer with {file_path=}")

    def parquet_writer_func(records: Iterator[Dict[str, Any]]) -> Iterator[str]:
        writer = None
        file_number = 1
        file_length = 0
        for batch in batched(records, row_group_length):
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None:
                formated_file_path = file_path
                if multi_file:
                    formated_file_path = file_path.format(i=file_number)
                logger.info(f"Writing to {formated_file_path=}")
                writer = pq.ParquetWriter(formated_file_path, table.schema)

            writer.write_table(table)
            file_length += len(batch)
            logger.info(
                f"Written {len(batch)} ({file_length} total) rows "
                f"to {formated_file_path}"
            )

            if max_file_length is not None and file_length >= max_file_length:
                writer.close()
                writer = None
                file_length = 0
                file_number += 1
                logger.info(f"Finished writing to {formated_file_path}")
                yield formated_file_path

        if writer is not None:
            writer.close()
            logger.info(f"Final file closed at {formated_file_path}")
            yield formated_file_path

    return parquet_writer_func
