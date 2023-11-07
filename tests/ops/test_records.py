import io
import json

from pipedata.core import StreamStart
from pipedata.ops.records import csv_records, json_records


def test_json_records() -> None:
    json1 = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    json2 = [{"a": 5, "b": 6}, {"a": 7, "b": 8}]

    file1 = io.BytesIO(json.dumps(json1).encode("utf-8"))
    file2 = io.BytesIO(json.dumps(json2).encode("utf-8"))

    result = StreamStart([file1, file2]).flat_map(json_records()).to_list()
    expected = json1 + json2
    assert result == expected


def test_csv_records() -> None:
    csv1 = "a,b\n1,2\n3,4"
    csv2 = "a,b\n5,6\n7,8"

    file1 = io.BytesIO(csv1.encode("utf-8"))
    file2 = io.BytesIO(csv2.encode("utf-8"))

    result = StreamStart([file1, file2]).flat_map(csv_records()).to_list()
    expected = [
        {"a": "1", "b": "2"},
        {"a": "3", "b": "4"},
        {"a": "5", "b": "6"},
        {"a": "7", "b": "8"},
    ]
    assert result == expected
