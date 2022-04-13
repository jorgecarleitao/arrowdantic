import arrowdantic
import pyarrow as pa


def test_int32():
    a = arrowdantic.Int32Array([1, 2])
    assert str(a) == "Int32[1, 2]"

    a = arrowdantic.Int32Array([1, None])
    assert str(a) == "Int32[1, None]"
    assert len(a) == 2


def test_uint32():
    a = arrowdantic.UInt32Array([1, 2])
    assert str(a) == "UInt32[1, 2]"

    a = arrowdantic.UInt32Array([1, None])
    assert str(a) == "UInt32[1, None]"
    assert len(a) == 2


def test_boolean():
    a = arrowdantic.BooleanArray([True, False])
    assert str(a) == "BooleanArray[true, false]"

    a = arrowdantic.BooleanArray([True, None])
    assert str(a) == "BooleanArray[true, None]"
    assert len(a) == 2


def test_logical():
    dt = arrowdantic.LogicalType(int)
    assert str(dt) == "Int64"

    dt = arrowdantic.LogicalType(bool)
    assert str(dt) == "Boolean"

    dt = arrowdantic.LogicalType("int32")
    assert str(dt) == "Int32"


def test_ipc_read():
    schema = pa.schema([pa.field('nums', pa.int32())])

    import io
    data = io.BytesIO()
    with pa.ipc.new_file(data, schema) as writer:
        batch = pa.record_batch([pa.array(range(3), type=pa.int32())], schema)
        writer.write(batch)
    data.seek(0)

    reader = arrowdantic.FileReader(data)
    chunk = next(reader)
    assert len(chunk) == 3
