import tempfile
import zipfile
from pathlib import Path

import pyarrow as pa  # type: ignore
import pytest

from pipedata.core import Stream, ops
from pipedata.ops.files import FilesReaderError, read_from_parquet, zipped_files


def test_zipped_files() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"

        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("test.txt", "Hello, world 1!")
            zip_file.writestr("test2.txt", "Hello, world 2!")
            zip_file.writestr("test3.txt", "Hello, world 3!")

        result = Stream([str(zip_path)]).then(zipped_files).to_list()

        assert result[0].name == "test.txt"
        assert result[1].name == "test2.txt"
        assert result[2].name == "test3.txt"


def test_zipped_file_contents() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"

        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("test.txt", "Hello, world 1!")
            zip_file.writestr("test2.txt", "Hello, world 2!")
            zip_file.writestr("test3.txt", "Hello, world 3!")

        result = (
            Stream([str(zip_path)])
            .then(zipped_files)
            .then(ops.mapping(lambda x: x.contents.read().decode("utf-8")))  # type: ignore  # TODO
            .to_list()
        )

        expected = [
            "Hello, world 1!",
            "Hello, world 2!",
            "Hello, world 3!",
        ]
        assert result == expected


def test_parquet_reading_simple() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "test.parquet"

        table = pa.Table.from_pydict(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
            }
        )
        pa.parquet.write_table(table, parquet_path)

        parquet_reader = read_from_parquet()
        result = Stream([str(parquet_path)]).then(parquet_reader).to_list()

        expected = [
            {"a": 1, "b": 4},
            {"a": 2, "b": 5},
            {"a": 3, "b": 6},
        ]
        assert result == expected


def test_parquet_reading_with_columns() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "test.parquet"

        table = pa.Table.from_pydict(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
            }
        )
        pa.parquet.write_table(table, parquet_path)

        parquet_reader = read_from_parquet(columns=["a"])
        result = Stream([str(parquet_path)]).then(parquet_reader).to_list()

        expected = [
            {"a": 1},
            {"a": 2},
            {"a": 3},
        ]
        assert result == expected


def test_parquet_reading_record_batch() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "test.parquet"

        table = pa.Table.from_pydict(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
            }
        )
        pa.parquet.write_table(table, parquet_path)

        parquet_reader = read_from_parquet(columns=["a"], return_as="recordbatch")
        result = Stream([str(parquet_path)]).then(parquet_reader).to_list()

        schema = pa.schema([("a", pa.int64())])
        a_array = pa.array([1, 2, 3])
        rb = pa.RecordBatch.from_arrays([a_array], schema=schema)
        assert result == [rb]


def test_parquet_reading_invalid_return_as() -> None:
    with pytest.raises(FilesReaderError):
        read_from_parquet(columns=["a"], return_as="unknown")  # type: ignore
