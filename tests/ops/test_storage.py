import tempfile
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore
import pytest

from pipedata.core import StreamStart
from pipedata.ops.storage import parquet_writer


def test_parquet_simple_storage() -> None:
    items = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 5, "b": 6},
        {"a": 7, "b": 8},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        output_path = temp_path / "test.parquet"

        result = StreamStart(items).flat_map(parquet_writer(str(output_path))).to_list()

        assert result == [str(output_path)]

        files = list(temp_path.glob("**/*"))
        assert files == [output_path]

        table = pq.read_table(output_path)
        assert table.to_pydict() == {
            "a": [1, 3, 5, 7],
            "b": [2, 4, 6, 8],
        }


def test_parquet_batched_storage() -> None:
    items = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 5, "b": 6},
        {"a": 7, "b": 8},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        output_path = temp_path / "test.parquet"

        result = (
            StreamStart(items)
            .flat_map(parquet_writer(str(output_path), row_group_length=2))
            .to_list()
        )

        assert result == [str(output_path)]

        files = list(temp_path.glob("**/*"))
        assert files == [output_path]

        table = pq.read_table(output_path)
        assert table.to_pydict() == {
            "a": [1, 3, 5, 7],
            "b": [2, 4, 6, 8],
        }


def test_parquet_multiple_files() -> None:
    items = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 5, "b": 6},
        {"a": 7, "b": 8},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        output_path = temp_path / "test_{i:04d}.parquet"

        result = (
            StreamStart(items)
            .flat_map(parquet_writer(str(output_path), max_file_length=2))
            .to_list()
        )

        assert result == [
            str(temp_path / "test_0001.parquet"),
            str(temp_path / "test_0002.parquet"),
        ]

        files = sorted(temp_path.glob("**/*"))
        expected_files = [
            temp_path / "test_0001.parquet",
            temp_path / "test_0002.parquet",
        ]
        assert files == expected_files

        table1 = pq.read_table(files[0])
        assert table1.to_pydict() == {
            "a": [1, 3],
            "b": [2, 4],
        }

        table2 = pq.read_table(files[1])
        assert table2.to_pydict() == {
            "a": [5, 7],
            "b": [6, 8],
        }

        table = pq.read_table(temp_path)
        assert table.to_pydict() == {
            "a": [1, 3, 5, 7],
            "b": [2, 4, 6, 8],
        }


def test_parquet_multiple_files_wrong_path() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        output_path = temp_path / "test.parquet"

        with pytest.raises(ValueError):  # noqa: PT011
            parquet_writer(str(output_path), max_file_length=2)
