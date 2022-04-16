import arrowdantic as ad
import pyarrow as pa
import pyarrow.parquet


def test_int32():
    a = ad.Int32Array([1, 2])
    assert str(a) == "Int32[1, 2]"
    assert a.type == ad.DataType.int32()

    a = ad.Int32Array([1, None])
    assert str(a) == "Int32[1, None]"
    assert len(a) == 2
    assert list(a) == [1, None]


def test_uint32():
    a = ad.UInt32Array([1, 2])
    assert str(a) == "UInt32[1, 2]"
    assert a.type == ad.DataType.uint32()

    a = ad.UInt32Array([1, None])
    assert str(a) == "UInt32[1, None]"
    assert len(a) == 2
    assert list(a) == [1, None]


def test_boolean():
    a = ad.BooleanArray([True, False])
    assert str(a) == "BooleanArray[true, false]"
    assert a.type == ad.DataType.bool()

    a = ad.BooleanArray([True, None])
    assert str(a) == "BooleanArray[true, None]"
    assert len(a) == 2
    assert list(a) == [True, None]


def test_string():
    a = ad.StringArray(["a", "b"])
    assert str(a) == "Utf8Array[a, b]"
    assert a.type == ad.DataType.string()

    a = ad.StringArray(["a", None])
    assert str(a) == "Utf8Array[a, None]"
    assert len(a) == 2
    assert list(a) == ["a", None]


def test_schema():
    fields = [ad.Field("c1", ad.DataType.int32(), True)]
    schema = ad.Schema(fields)
    assert schema.fields == fields


def test_ipc_read():
    arrays = [
        pa.array([True, None, False], type=pa.bool_()),
        pa.array(range(3), type=pa.int8()),
        pa.array(range(3), type=pa.int32()),
        pa.array(range(3), type=pa.int64()),
        pa.array(["a", None, "c"], type=pa.string()),
        pa.array(["a", None, "c"], type=pa.large_string()),
        pa.array([b"a", None, b"c"], type=pa.binary()),
        pa.array([b"a", None, b"c"], type=pa.large_binary()),
        pa.array([1.2, None, 3.4], type=pa.float32()),
        pa.array([1.2, None, 3.4], type=pa.float64()),
    ]

    schema = pa.schema(
        [pa.field(f"c{i}", array.type) for i, array in enumerate(arrays)]
    )

    import io

    data = io.BytesIO()
    with pa.ipc.new_file(data, schema) as writer:
        batch = pa.record_batch(arrays, schema)
        writer.write(batch)
    data.seek(0)

    reader = ad.ArrowFileReader(data)
    chunk = next(reader)
    assert len(chunk) == 3
    arrays = chunk.arrays()
    assert arrays[0] == ad.BooleanArray([True, None, False])
    assert arrays[1] == ad.Int8Array(list(range(3)))
    assert arrays[2] == ad.Int32Array(list(range(3)))
    assert arrays[3] == ad.Int64Array(list(range(3)))
    assert arrays[4] == ad.StringArray(["a", None, "c"])
    assert arrays[5] == ad.LargeStringArray(["a", None, "c"])
    assert arrays[6] == ad.BinaryArray([b"a", None, b"c"])
    assert arrays[7] == ad.LargeBinaryArray([b"a", None, b"c"])
    assert arrays[8] == ad.Float32Array([1.2, None, 3.4])
    assert arrays[9] == ad.Float64Array([1.2, None, 3.4])


def test_parquet_read():
    arrays = [
        pa.array([True, None, False], type=pa.bool_()),
        pa.array(range(3), type=pa.int8()),
        pa.array(range(3), type=pa.int32()),
        pa.array(range(3), type=pa.int64()),
        pa.array(["a", None, "c"], type=pa.string()),
        pa.array(["a", None, "c"], type=pa.large_string()),
        pa.array([b"a", None, b"c"], type=pa.binary()),
        pa.array([b"a", None, b"c"], type=pa.large_binary()),
        pa.array([1.2, None, 3.4], type=pa.float32()),
        pa.array([1.2, None, 3.4], type=pa.float64()),
    ]

    schema = pa.schema(
        [pa.field(f"c{i}", array.type) for i, array in enumerate(arrays)]
    )

    import io

    data = io.BytesIO()
    pa.parquet.write_table(pa.table(arrays, schema), data)
    data.seek(0)

    reader = ad.ParquetFileReader(data)
    chunk = next(reader)
    assert len(chunk) == 3
    arrays = chunk.arrays()
    assert arrays[0] == ad.BooleanArray([True, None, False])
    assert arrays[1] == ad.Int8Array(list(range(3)))
    assert arrays[2] == ad.Int32Array(list(range(3)))
    assert arrays[3] == ad.Int64Array(list(range(3)))
    assert arrays[4] == ad.StringArray(["a", None, "c"])
    assert arrays[5] == ad.LargeStringArray(["a", None, "c"])
    assert arrays[6] == ad.BinaryArray([b"a", None, b"c"])
    assert arrays[7] == ad.LargeBinaryArray([b"a", None, b"c"])
    assert arrays[8] == ad.Float32Array([1.2, None, 3.4])
    assert arrays[9] == ad.Float64Array([1.2, None, 3.4])


def test_ipc_round_trip():
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


def test_parquet_round_trip():
    original_arrays = [ad.UInt32Array([1, None])]

    schema = ad.Schema(
        [ad.Field(f"c{i}", array.type, True) for i, array in enumerate(original_arrays)]
    )

    import io

    data = io.BytesIO()
    with ad.ParquetFileWriter(data, schema) as writer:
        writer.write(ad.Chunk(original_arrays))
    data.seek(0)

    reader = ad.ParquetFileReader(data)
    chunk = next(reader)
    assert chunk.arrays() == original_arrays


def test_sql_roundtrip():
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
