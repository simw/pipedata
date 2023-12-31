#!/usr/bin/env bash

set -o errexit  # Abort on non-zero exit status
set -o nounset  # Abort on unbound variable
set -o pipefail # Abort on non-zero exit in pipeline

main() {
    PYTHON_MINOR_VERSION=$(poetry run python -c 'import sys; version=sys.version_info[:3]; print("{1}".format(*version))')
    echo "Python minor version: $PYTHON_MINOR_VERSION"

    # The errors are mostly / all installation errors,
    # about building from source. Could lower
    # the requirements if able to build from source.
    if (( $PYTHON_MINOR_VERSION < "11" )); then
        poetry run pip install pyarrow==9.0.0
        poetry run python -m pytest

        poetry run pip install pyarrow==10.0.0
        poetry run python -m pytest
    fi

    if (( $PYTHON_MINOR_VERSION < "12" )); then
        poetry run pip install pyarrow==11.0.0
        poetry run python -m pytest

        poetry run pip install pyarrow==13.0.0
        poetry run python -m pytest

        poetry run pip install fsspec==0.9.0
        poetry run python -m pytest
    fi

	poetry run pip install pyarrow==14.0.0
	poetry run python -m pytest

	poetry run pip install ijson==3.0.0
	poetry run python -m pytest

    poetry run pip install fsspec==2022.1.0
    poetry run python -m pytest
}

main
