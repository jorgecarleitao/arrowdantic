import arrowdantic
import pyarrow as pa
import pyarrow.parquet


def test_int32():
    a = arrowdantic.Int32Array([1, 2])
    assert str(a) == "Int32[1, 2]"

    a = arrowdantic.Int32Array([1, None])
    assert str(a) == "Int32[1, None]"
    assert len(a) == 2
    assert list(a) == [1, None]


def test_uint32():
    a = arrowdantic.UInt32Array([1, 2])
    assert str(a) == "UInt32[1, 2]"

    a = arrowdantic.UInt32Array([1, None])
    assert str(a) == "UInt32[1, None]"
    assert len(a) == 2
    assert list(a) == [1, None]


def test_boolean():
    a = arrowdantic.BooleanArray([True, False])
    assert str(a) == "BooleanArray[true, false]"

    a = arrowdantic.BooleanArray([True, None])
    assert str(a) == "BooleanArray[true, None]"
    assert len(a) == 2
    assert list(a) == [True, None]


def test_string():
    a = arrowdantic.StringArray(["a", "b"])
    assert str(a) == "Utf8Array[a, b]"

    a = arrowdantic.StringArray(["a", None])
    assert str(a) == "Utf8Array[a, None]"
    assert len(a) == 2
    assert list(a) == ["a", None]


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

    schema = pa.schema([
        pa.field(f'c{i}', array.type)
        for i, array in enumerate(arrays)
    ])

    import io
    data = io.BytesIO()
    with pa.ipc.new_file(data, schema) as writer:
        batch = pa.record_batch(arrays, schema)
        writer.write(batch)
    data.seek(0)

    reader = arrowdantic.ArrowFileReader(data)
    chunk = next(reader)
    assert len(chunk) == 3
    arrays = chunk.arrays()
    assert arrays[0] == arrowdantic.BooleanArray([True, None, False])
    assert arrays[1] == arrowdantic.Int8Array(list(range(3)))
    assert arrays[2] == arrowdantic.Int32Array(list(range(3)))
    assert arrays[3] == arrowdantic.Int64Array(list(range(3)))
    assert arrays[4] == arrowdantic.StringArray(["a", None, "c"])
    assert arrays[5] == arrowdantic.LargeStringArray(["a", None, "c"])
    assert arrays[6] == arrowdantic.BinaryArray([b"a", None, b"c"])
    assert arrays[7] == arrowdantic.LargeBinaryArray([b"a", None, b"c"])
    assert arrays[8] == arrowdantic.Float32Array([1.2, None, 3.4])
    assert arrays[9] == arrowdantic.Float64Array([1.2, None, 3.4])


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

    schema = pa.schema([
        pa.field(f'c{i}', array.type)
        for i, array in enumerate(arrays)
    ])

    import io
    data = io.BytesIO()
    pa.parquet.write_table(pa.table(arrays, schema), data)
    data.seek(0)

    reader = arrowdantic.ParquetFileReader(data)
    chunk = next(reader)
    assert len(chunk) == 3
    arrays = chunk.arrays()
    assert arrays[0] == arrowdantic.BooleanArray([True, None, False])
    assert arrays[1] == arrowdantic.Int8Array(list(range(3)))
    assert arrays[2] == arrowdantic.Int32Array(list(range(3)))
    assert arrays[3] == arrowdantic.Int64Array(list(range(3)))
    assert arrays[4] == arrowdantic.StringArray(["a", None, "c"])
    assert arrays[5] == arrowdantic.LargeStringArray(["a", None, "c"])
    assert arrays[6] == arrowdantic.BinaryArray([b"a", None, b"c"])
    assert arrays[7] == arrowdantic.LargeBinaryArray([b"a", None, b"c"])
    assert arrays[8] == arrowdantic.Float32Array([1.2, None, 3.4])
    assert arrays[9] == arrowdantic.Float64Array([1.2, None, 3.4])
