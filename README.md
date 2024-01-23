# pipedata

Chained operations in Python, applied to data processing.

## Installation

To install with all optional dependencies:

```bash
pip install pipedata[ops]
```

If you only want the core functionality (building pipelines), and not
the data processing applications, then:

```bash
pip install pipedata
```

## Examples

### Chaining Data Operations

pipedata.ops provides some operations for streaming data through memory.

```py
import json
import zipfile

import pyarrow.parquet as pq

from pipedata.core import Stream
from pipedata.ops import json_records, parquet_writer, zipped_files


data1 = [
    {"col1": 1, "col2": "Hello"},
    {"col1": 2, "col2": "world"},
]
data2 = [
    {"col1": 3, "col2": "!"},
]

with zipfile.ZipFile("test_input.json.zip", "w") as zipped:
    zipped.writestr("file1.json", json.dumps(data1))
    zipped.writestr("file2.json", json.dumps(data2))

result = (
    Stream(["test_input.json.zip"])
    .then(zipped_files)
    .then(json_records())
    .then(parquet_writer("test_output.parquet"))
    .to_list()
)

table = pq.read_table("test_output.parquet")
print(table.to_pydict())
#> {'col1': [1, 2, 3], 'col2': ['Hello', 'world', '!']}
```

Alternatively, you can construct the pipeline as a chain:

```py
import pyarrow.parquet as pq

from pipedata.core import Chain, Stream
from pipedata.ops import json_records, parquet_writer, zipped_files

# Running this after input file created in above example
chain = (
    Chain()
    .then(zipped_files)
    .then(json_records())
    .then(parquet_writer("test_output_2.parquet"))
)
result = Stream(["test_input.json.zip"]).then(chain).to_list()
table = pq.read_table("test_output_2.parquet")
print(table.to_pydict())
#> {'col1': [1, 2, 3], 'col2': ['Hello', 'world', '!']}

```

### Core Framework

The core framework provides the building blocks for chaining operations.

Running a stream:
```py
from pipedata.core import Stream, ops


result = (
    Stream(range(10))
    .then(ops.filtering(lambda x: x % 2 == 0))
    .then(ops.mapping(lambda x: x ^ 2))
    .then(ops.batching(lambda x: x, 2))
    .to_list()
)
print(result)
#> [(2, 0), (6, 4), (10,)]
```

Creating a chain and then using it, this time using the
pipe notation:
```py
import json
from pipedata.core import Chain, Stream, ops


chain = (
    Chain()
    | ops.filtering(lambda x: x % 2 == 0)
    | ops.mapping(lambda x: x ^ 2)
    | ops.batching(lambda x: sum(x), 2)
)
print(Stream(range(10)).then(chain).to_list())
#> [2, 10, 10]
print(json.dumps(chain.get_counts(), indent=4))
#> [
#>     {
#>         "name": "_identity",
#>         "inputs": 10,
#>         "outputs": 10
#>     },
#>     {
#>         "name": "<lambda>",
#>         "inputs": 10,
#>         "outputs": 5
#>     },
#>     {
#>         "name": "<lambda>",
#>         "inputs": 5,
#>         "outputs": 5
#>     },
#>     {
#>         "name": "<lambda>",
#>         "inputs": 5,
#>         "outputs": 3
#>     }
#> ]
```

## Similar Functionality

- Python has built in functionality for building iterators

- [LangChain](https://www.langchain.com/) implements chained operations using its 
[Runnable protocol](https://python.langchain.com/docs/expression_language/interface)
