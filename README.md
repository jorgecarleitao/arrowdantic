# Welcome to arrowdantic

Arrowdantic is a small Python library backed by a
[mature Rust implementation](https://github.com/jorgecarleitao/arrow2) of Apache Arrow
that can interoperate with
* [Parquet](https://parquet.apache.org/)
* [Apache Arrow](https://arrow.apache.org/) and 
* [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity) (databases).

For simple (but data-heavy) data engineering tasks, this package essentially replaces
`pyarrow`: it supports reading from and writing to Parquet, Arrow at the same or
higher performance and higher safety (e.g. no segfaults).

Furthermore, it supports reading from and writing to ODBC compliant databases at
the same or higher performance than [`turbodbc`](https://turbodbc.readthedocs.io/en/latest/).

This package is particularly suitable for environments such as AWS Lambda -
it takes 8M of disk space, compared to 82M taken by pyarrow.

## Features

* declare and access Arrow-backed arrays (integers, floats, boolean, string, binary)
* read from and write to Apache Arrow IPC file
* read from and write to Apache Parquet
* read from and write to ODBC-compliant databases (e.g. postgres, mongoDB)

## Examples

### Use parquet

```python
import io
import arrowdantic as ad

original_arrays = [ad.UInt32Array([1, None])]

schema = ad.Schema(
    [ad.Field(f"c{i}", array.type, True) for i, array in enumerate(original_arrays)]
)

data = io.BytesIO()
with ad.ParquetFileWriter(data, schema) as writer:
    writer.write(ad.Chunk(original_arrays))
data.seek(0)

reader = ad.ParquetFileReader(data)
chunk = next(reader)
assert chunk.arrays() == original_arrays
```

### Use Arrow files

```python
import arrowdantic as ad

original_arrays = [ad.UInt32Array([1, None])]

schema = ad.Schema(
    [ad.Field(f"c{i}", array.type, True) for i, array in enumerate(original_arrays)]
)

import io

data = io.BytesIO()
with ad.ArrowFileWriter(data, schema) as writer:
    writer.write(ad.Chunk(original_arrays))
data.seek(0)

reader = ad.ArrowFileReader(data)
chunk = next(reader)
assert chunk.arrays() == original_arrays
```

### Use ODBC

```python
import arrowdantic as ad


arrays = [ad.Int32Array([1, None]), ad.StringArray(["aa", None])]

with ad.ODBCConnector(r"Driver={SQLite3};Database=sqlite-test.db") as con:
    # create an empty table with a schema
    con.execute("DROP TABLE IF EXISTS example;")
    con.execute("CREATE TABLE example (c1 INT, c2 TEXT);")

    # insert the arrays
    con.write("INSERT INTO example (c1, c2) VALUES (?, ?)", ad.Chunk(arrays))

    # read the arrays
    with con.execute("SELECT c1, c2 FROM example", 1024) as chunks:
        assert chunks.fields() == [
            ad.Field("c1", ad.DataType.int32(), True),
            ad.Field("c2", ad.DataType.string(), True),
        ]
        chunk = next(chunks)
assert chunk.arrays() == arrays
```

### Use timezones

This package fully supports datetime and conversions between them and arrow:

```python
import arrowdantic as ad


dt = datetime.datetime(
    year=2021,
    month=1,
    day=1,
    hour=1,
    minute=1,
    second=1,
    microsecond=1,
    tzinfo=datetime.timezone.utc,
)
a = ad.TimestampArray([dt, None])
assert (
    str(a)
    == 'Timestamp(Microsecond, Some("+00:00"))[2021-01-01 01:01:01.000001 +00:00, None]'
)
assert list(a) == [dt, None]
assert a.type == ad.DataType.timestamp(datetime.timezone.utc)
```
