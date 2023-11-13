import tempfile
import zipfile
from pathlib import Path

from pipedata.core import StreamStart
from pipedata.ops.files import zipped_files


def test_zipped_files() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"

        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("test.txt", "Hello, world 1!")
            zip_file.writestr("test2.txt", "Hello, world 2!")
            zip_file.writestr("test3.txt", "Hello, world 3!")

        result = StreamStart([str(zip_path)]).flat_map(zipped_files).to_list()

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
            StreamStart([str(zip_path)])
            .flat_map(zipped_files)
            .map(lambda x: x.contents.read().decode("utf-8"))
            .to_list()
        )

        expected = [
            "Hello, world 1!",
            "Hello, world 2!",
            "Hello, world 3!",
        ]
        assert result == expected
