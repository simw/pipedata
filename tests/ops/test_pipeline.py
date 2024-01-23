import json
import tempfile
import zipfile
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore

from pipedata.core import Stream
from pipedata.ops import json_records, parquet_writer, zipped_files


def test_zipped_files() -> None:
    data1 = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
    ]
    data2 = [
        {"a": 5, "b": 6},
        {"a": 7, "b": 8},
    ]
    data3 = [
        {"a": 9, "b": 10},
        {"a": 11, "b": 12},
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        zip_path = temp_path / "test.zip"
        output_path = temp_path / "output.parquet"

        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("test.txt", json.dumps(data1))
            zip_file.writestr("test2.txt", json.dumps(data2))
            zip_file.writestr("test3.txt", json.dumps(data3))

        result = (
            Stream([str(zip_path)])
            .flat_map(zipped_files)
            .map(lambda x: x.contents)
            .flat_map(json_records())
            .flat_map(parquet_writer(str(output_path)))
            .to_list()
        )

        expected = [str(output_path)]
        assert result == expected

        files = list(temp_path.glob("**/*"))
        assert sorted(files) == sorted([output_path, zip_path])

        table = pq.read_table(output_path)
        assert table.to_pydict() == {
            "a": [1, 3, 5, 7, 9, 11],
            "b": [2, 4, 6, 8, 10, 12],
        }
