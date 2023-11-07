# pipedata

Chained operations in Python, applied to data processing.

## Installation

```bash
pip install pipedata
```

## An Example

### Core Framework

The core framework provides the building blocks for chaining operations.

Running a stream:
```py
from pipedata.core import StreamStart


result = (
    StreamStart(range(10))
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x ^ 2)
    .map_tuple(lambda x: x, 2)
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
    .map_tuple(lambda x: sum(x), 2)
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
