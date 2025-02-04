import json
import tempfile
import zipfile
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore

from pipedata.core import Chain, Stream, ops
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
            .then(zipped_files)
            .then(ops.mapping(lambda x: x.contents))  # type: ignore  # TODO
            .then(json_records())
            .then(parquet_writer(str(output_path)))
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


def test_zipped_file_contents() -> None:
    contents = """
    <xml>
        <name>John</name>
        <age>30</age>
    </xml>
    <xml>
        <name>Smith</name>
        <age>40</age>
    </xml>
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"

        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("test.txt", contents)
            zip_file.writestr("test2.txt", contents)
            zip_file.writestr("test3.txt", contents)

        extract_xmls = (
            Chain()
            .then(ops.grouper(starter=lambda line: line.strip().startswith(b"<xml")))
            .then(ops.mapping(lambda x: b"\n".join(x)))
            .then(ops.filtering(lambda x: x.strip() != b""))
        )

        result = (
            Stream([str(zip_path)])
            .then(zipped_files)
            .then(ops.mapping(lambda x: x.contents))
            .then(ops.chain_iterables())
            .then(extract_xmls)
            .to_list()
        )
        assert len(result) == 6
