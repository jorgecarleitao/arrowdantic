from typing import List, Optional

import arrowdantic_internal


class DataType:
    __slots__ = ("_dt",)

    @classmethod
    def int32(_) -> "DataType":
        return DataType._from_type(arrowdantic_internal.DataType.int32())

    @classmethod
    def uint32(_) -> "DataType":
        return DataType._from_type(arrowdantic_internal.DataType.uint32())

    @classmethod
    def bool(_) -> "DataType":
        return DataType._from_type(arrowdantic_internal.DataType.bool())

    @classmethod
    def string(_) -> "DataType":
        return DataType._from_type(arrowdantic_internal.DataType.string())

    @classmethod
    def _from_type(cls, dt: arrowdantic_internal.DataType) -> "DataType":
        a = DataType()
        a._dt = dt
        return a

    def __repr__(self):
        return self._dt.__repr__()

    def __eq__(self, o: "DataType") -> bool:
        return o._dt == self._dt


class Field:
    __slots__ = ("_field",)

    def __init__(self, name: str, data_type: DataType, is_nullable: bool):
        self._field = arrowdantic_internal.Field(name, data_type._dt, is_nullable)

    @classmethod
    def _from_field(cls, f: arrowdantic_internal.Field) -> "Field":
        self = Field("", DataType.int32(), True)
        self._field = f
        return self

    @property
    def name(self) -> str:
        return self._field.name

    @property
    def data_type(self) -> DataType:
        return DataType._from_type(self._field.data_type)

    @property
    def is_nullable(self) -> bool:
        return self._field.is_nullable

    def __repr__(self):
        return self._field.__repr__()

    def __eq__(self, o: "Field") -> bool:
        return o._field == self._field


class Schema:
    __slots__ = ("_schema",)

    def __init__(self, fields: List[Field]):
        self._schema = arrowdantic_internal.Schema([f._field for f in fields])

    @property     
    def fields(self):
        return [Field._from_field(f) for f in self._schema.fields]


class Array:
    __slots__ = ("_array",)

    @classmethod
    def _from_array(cls, array):
        a = cls()
        a._array = array
        return a

    @property
    def type(self):
        return DataType._from_type(self._array.type)

    def __repr__(self):
        return self._array.__repr__()

    def __len__(self) -> int:
        return self._array.__len__()

    def __eq__(self, o: "Array") -> bool:
        return o._array == self._array

    def __iter__(self):
        return self._array.__iter__()


class Int8Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.Int8Array(values)


class Int32Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.Int32Array(values)


class Int64Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.Int64Array(values)


class Float32Array(Array):
    def __init__(self, values: List[Optional[float]]):
        self._array = arrowdantic_internal.Float32Array(values)


class Float64Array(Array):
    def __init__(self, values: List[Optional[float]]):
        self._array = arrowdantic_internal.Float64Array(values)


class UInt32Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.UInt32Array(values)


class UInt64Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.UInt64Array(values)


class BooleanArray(Array):
    def __init__(self, values: List[Optional[bool]]):
        self._array = arrowdantic_internal.BooleanArray(values)


class StringArray(Array):
    def __init__(self, values: List[Optional[str]]):
        self._array = arrowdantic_internal.StringArray(values)


class LargeStringArray(Array):
    def __init__(self, values: List[Optional[str]]):
        self._array = arrowdantic_internal.LargeStringArray(values)


class BinaryArray(Array):
    def __init__(self, values: List[Optional[bytes]]):
        self._array = arrowdantic_internal.BinaryArray(values)


class LargeBinaryArray(Array):
    def __init__(self, values: List[Optional[bytes]]):
        self._array = arrowdantic_internal.LargeBinaryArray(values)


class Chunk:
    def __init__(self, arrays: List[Array]):
        self._chunk = arrowdantic_internal.Chunk([x._array for x in arrays])

    @staticmethod
    def _from_chunk(chunk: arrowdantic_internal.Chunk) -> "Chunk":
        a = Chunk([])
        a._chunk = chunk
        return a

    def arrays(self) -> List[Array]:
        return [Array._from_array(array) for array in self._chunk.arrays()]

    def __repr__(self):
        return self._chunk.__repr__()

    def __len__(self) -> int:
        return self._chunk.__len__()


class ArrowFileReader:
    """
    An iterator of ``Chunk``, each corresponding to a group of arrays from an Arrow IPC file.
    """
    def __init__(self, path_or_obj):
        self._reader = arrowdantic_internal.ArrowFileReader(path_or_obj)

    def __iter__(self):
        return self

    def __next__(self):
        return Chunk._from_chunk(self._reader.__next__())


class ArrowFileWriter:
    """
    Context manager to write ``Chunk``s to Arrow IPC file.
    """
    __slots__ = ("_writer", "_schema", "_path")

    def __init__(self, path_or_obj, schema: Schema):
        self._path = path_or_obj
        self._schema = schema
        self._writer = None

    def __enter__(self) -> "ArrowFileWriter":
        self._writer = arrowdantic_internal.ArrowFileWriter(self._path, self._schema._schema)
        return self

    def write(self, chunk: Chunk):
        self._writer.write(chunk._chunk)

    def __exit__(self, _, __, ___):
        self._writer.__exit__()


class ParquetFileReader:
    """
    An iterator of ``Chunk``, each corresponding to a row group from a Parquet file.
    """
    def __init__(self, path_or_obj):
        self._reader = arrowdantic_internal.ParquetFileReader(path_or_obj)

    def __iter__(self):
        return self

    def __next__(self):
        return Chunk._from_chunk(self._reader.__next__())
