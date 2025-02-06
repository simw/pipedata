#!/usr/bin/env bash

set -o errexit  # Abort on non-zero exit status
set -o nounset  # Abort on unbound variable
set -o pipefail # Abort on non-zero exit in pipeline

main() {
    PYTHON_MINOR_VERSION=$(poetry run python -c 'import sys; version=sys.version_info[:3]; print("{1}".format(*version))')
    echo "Python minor version: $PYTHON_MINOR_VERSION"

    if (( $PYTHON_MINOR_VERSION < "12" )); then
        poetry run pip install fsspec==0.9.0
        poetry run python -m pytest
    fi

	poetry run pip install pyarrow==16.0.0
	poetry run python -m pytest

	poetry run pip install ijson==3.0.0
	poetry run python -m pytest

    poetry run pip install fsspec==2022.1.0
    poetry run python -m pytest
}

main
