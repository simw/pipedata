from typing import Any, Callable, Dict, Iterator, Optional

import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from pipedata.core.chain import batched

# Option to accumulate the pyarrow table more frequently
# so that doesn't need whole list(dict) and pyarrow table
# in memory at the same time

# Option to hae row_group_length and max_file_length dpendent
# on size of data, as opposed to number of just numbers of rows.
# Can combine this with the existing settings, so runs
# at the smaller of the two.


def parquet_writer(
    file_path: str,
    schema: Optional[pa.Schema] = None,
    row_group_length: Optional[int] = None,
    max_file_length: Optional[int] = None,
) -> Callable[[Iterator[Dict[str, Any]]], Iterator[str]]:
    if row_group_length is None and max_file_length is not None:
        row_group_length = max_file_length

    if max_file_length is not None:
        if file_path.format(i=1) == file_path:
            msg = "When (possibly) writing to multiple files (as the file_length"
            msg += " argument is not None), the file_path argument must be a"
            msg += " format string that contains a format specifier for the file."
            raise ValueError(msg)

    def parquet_writer_func(records: Iterator[Dict[str, Any]]) -> Iterator[str]:
        writer = None
        file_number = 1
        file_length = 0
        for batch in batched(records, row_group_length):
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None:
                formated_file_path = file_path
                if max_file_length is not None:
                    formated_file_path = file_path.format(i=file_number)
                writer = pq.ParquetWriter(formated_file_path, table.schema)

            writer.write_table(table)
            file_length += len(batch)

            if max_file_length is not None and file_length >= max_file_length:
                writer.close()
                writer = None
                file_length = 0
                file_number += 1
                yield formated_file_path

        if writer is not None:
            writer.close()
            yield formated_file_path

    return parquet_writer_func
