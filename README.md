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

from pipedata.core import StreamStart
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
    StreamStart(["test_input.json.zip"])
    .flat_map(zipped_files)
    .flat_map(json_records())
    .flat_map(parquet_writer("test_output.parquet"))
    .to_list()
)

table = pq.read_table("test_output.parquet")
print(table.to_pydict())
#> {'col1': [1, 2, 3], 'col2': ['Hello', 'world', '!']}
```

Alternatively, you can construct the pipeline as a chain:

```py
import pyarrow.parquet as pq

from pipedata.core import ChainStart, StreamStart
from pipedata.ops import json_records, parquet_writer, zipped_files

# Running this after input file created in above example
chain = (
    ChainStart()
    .flat_map(zipped_files)
    .flat_map(json_records())
    .flat_map(parquet_writer("test_output_2.parquet"))
)
result = StreamStart(["test_input.json.zip"]).flat_map(chain).to_list()
table = pq.read_table("test_output_2.parquet")
print(table.to_pydict())
#> {'col1': [1, 2, 3], 'col2': ['Hello', 'world', '!']}

```

### Core Framework

The core framework provides the building blocks for chaining operations.

Running a stream:
```py
from pipedata.core import StreamStart


result = (
    StreamStart(range(10))
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x ^ 2)
    .batched_map(lambda x: x, 2)
    .to_list()
)
print(result)
#> [(2, 0), (6, 4), (10,)]
```

Creating a chain and then using it:
```py
import json
from pipedata.core import ChainStart, Stream, StreamStart


chain = (
    ChainStart()
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x ^ 2)
    .batched_map(lambda x: sum(x), 2)
)
print(Stream(range(10), chain).to_list())
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
print(StreamStart(range(10)).flat_map(chain).to_list())
#> [2, 10, 10]
```

## Similar Functionality

- Python has built in functionality for building iterators

- [LangChain](https://www.langchain.com/) implements chained operations using its 
[Runnable protocol](https://python.langchain.com/docs/expression_language/interface)
