# Welcome to arrowdantic

Arrowdantic is a small Python library backed by a Rust implementation of Apache Arrow
to read Parquet and Arrow IPC files to Python.

Its main differences vs pyarrow are:
* it is quite small (3Mb vs 90Mb)
* faster
* likely safer (no segfaults, core dumps, buffer overflows, etc.)
* it is type-hinted
* it has a much smaller subset of its functionality
  * basic arrays (integers, floats, boolean, string, binary)
  * read Apache Arrow IPC file
  * read Apache Parquet

## Example 

```python
import io

import arrowdantic as ad
# pyarrow is not needed; we just use it here to write a parquet file for the example
import pyarrow as pa

def _write_a_parquet() -> io.BytesIO:
    arrays = [
        pa.array([True, None, False], type=pa.bool_()),
    ]

    schema = pa.schema([
        pa.field(f'c{i}', array.type)
        for i, array in enumerate(arrays)
    ])

    import io
    data = io.BytesIO()
    pa.parquet.write_table(pa.table(arrays, schema), data)
    data.seek(0)
    return data


parquet_file = _write_a_parquet()

reader = arrowdantic.ParquetFileReader(parquet_file)
chunk = next(reader)
assert len(chunk) == 3
arrays = chunk.arrays()
assert arrays[0] == arrowdantic.BooleanArray([True, None, False])
assert list(arrays[0]) == [True, None, False]
```
