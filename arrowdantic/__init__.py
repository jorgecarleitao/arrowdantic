"""
A Python library written in Rust to read from and write to Apache Arrow IPC (also known as feather)
and Apache Parquet.
"""
from typing import Iterable, List, Optional
import datetime

import _arrowdantic_internal


class DataType:
    """
    Arrow's representation of logical types.

    This class contains multiple class methods to initialize valid logical types
    """

    __slots__ = ("_dt",)

    @classmethod
    def bool(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.bool())

    @classmethod
    def int8(cls) -> "DataType":
        """Returns ``DataType`` representing a 8-bit signed integer"""
        return cls._from_type(_arrowdantic_internal.DataType.int8())

    @classmethod
    def int16(cls) -> "DataType":
        """Returns ``DataType`` representing a 16-bit signed integer"""
        return cls._from_type(_arrowdantic_internal.DataType.int16())

    @classmethod
    def int32(cls) -> "DataType":
        """Returns ``DataType`` representing a 32-bit signed integer"""
        return cls._from_type(_arrowdantic_internal.DataType.int32())

    @classmethod
    def int64(cls) -> "DataType":
        """Returns ``DataType`` representing a 64-bit signed integer"""
        return cls._from_type(_arrowdantic_internal.DataType.int64())

    @classmethod
    def uint8(cls) -> "DataType":
        """Returns ``DataType`` representing a 8-bit unsigned integer"""
        return cls._from_type(_arrowdantic_internal.DataType.uint8())

    @classmethod
    def uint16(cls) -> "DataType":
        """Returns ``DataType`` representing a 16-bit unsigned integer"""
        return cls._from_type(_arrowdantic_internal.DataType.uint16())

    @classmethod
    def uint32(cls) -> "DataType":
        """Returns ``DataType`` representing a 32-bit unsigned integer"""
        return cls._from_type(_arrowdantic_internal.DataType.uint32())

    @classmethod
    def uint64(cls) -> "DataType":
        """Returns ``DataType`` representing a 64-bit unsigned integer"""
        return cls._from_type(_arrowdantic_internal.DataType.uint64())

    @classmethod
    def string(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.string())

    @classmethod
    def large_string(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.large_string())

    @classmethod
    def binary(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.binary())

    @classmethod
    def large_binary(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.large_binary())

    @classmethod
    def timestamp(cls, tz: Optional[datetime.tzinfo] = None) -> "DataType":
        if tz:
            tz = _format_offset(tz.utcoffset(None))
        return cls._from_type(_arrowdantic_internal.DataType.timestamp(tz))

    @classmethod
    def date(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.date())

    @classmethod
    def time(cls) -> "DataType":
        return cls._from_type(_arrowdantic_internal.DataType.time())

    @classmethod
    def _from_type(cls, dt: _arrowdantic_internal.DataType) -> "DataType":
        a = DataType()
        a._dt = dt
        return a

    def __repr__(self):
        return self._dt.__repr__()

    def __eq__(self, o: "DataType") -> bool:
        return o._dt == self._dt


class Field:
    """
    Arrow's representation of a column. It is composed by
    * a name
    * a DataType
    * its nullability
    """

    __slots__ = ("_field",)

    def __init__(self, name: str, data_type: DataType, is_nullable: bool):
        self._field = _arrowdantic_internal.Field(name, data_type._dt, is_nullable)

    @classmethod
    def _from_field(cls, f: _arrowdantic_internal.Field) -> "Field":
        self = Field("", DataType.int32(), True)
        self._field = f
        return self

    @property
    def name(self) -> str:
        """The fields' name"""
        return self._field.name

    @property
    def data_type(self) -> DataType:
        """The fields' ``DataType``"""
        return DataType._from_type(self._field.data_type)

    @property
    def is_nullable(self) -> bool:
        """The fields' nullability"""
        return self._field.is_nullable

    def __repr__(self):
        return self._field.__repr__()

    def __eq__(self, o: "Field") -> bool:
        return o._field == self._field


class Schema:
    """A collection of ``Field``s"""

    __slots__ = ("_schema",)

    def __init__(self, fields: List[Field]):
        self._schema = _arrowdantic_internal.Schema([f._field for f in fields])

    @property
    def fields(self):
        """The fields"""
        return [Field._from_field(f) for f in self._schema.fields]


class Array:
    """An ``Array`` is an immutable, fixed length, Arrow-aligned sequence of optional elements.
    Different implementations represent different logical types (e.g. integers, booleans, strings)"""

    __slots__ = ("_array",)

    @classmethod
    def _from_array(cls, array):
        a = cls()
        a._array = array
        return a

    @property
    def type(self) -> DataType:
        """The array's ``DataType``."""
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
    """An array of 8-bit signed integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.Int8Array(values)


class Int16Array(Array):
    """An array of 16-bit signed integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.Int16Array(values)


class Int32Array(Array):
    """An array of 32-bit signed integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.Int32Array(values)


class Int64Array(Array):
    """An array of 64-bit signed integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.Int64Array(values)


class TimestampArray(Int64Array):
    """An array of 64-bit signed integers each representing a datetime with the same timezone"""

    __slots__ = ("_tz",)

    def __init__(self, values: List[Optional[datetime.datetime]]):
        def _transform(value: Optional[datetime.datetime]):
            if value is None:
                return None
            else:
                seconds = value.timestamp()
                microseconds = int(seconds * 10**6)
                return microseconds

        if not values:
            tz = None
            self._tz = None
        else:
            tz = values[0].timetz()
            self._tz = tz.tzinfo
            tz = _format_offset(tz.utcoffset())

        values = list(map(_transform, values))
        self._array = _arrowdantic_internal.Int64Array.from_ts_us(values, tz)

    def __iter__(self):
        return TimestampIterator(self._array.__iter__(), self._tz)


class TimestampIterator:
    __slots__ = ("_iter", "_tz")

    def __init__(self, iter, tz):
        self._iter = iter
        self._tz = tz

    def __iter__(self):
        return self

    def __next__(self):
        dt_i64 = next(self._iter)
        if dt_i64:
            return datetime.datetime.fromtimestamp(dt_i64 / 10**6, self._tz)


_DATE_EPOCH = datetime.datetime.utcfromtimestamp(0).date()

class DateArray(Int32Array):
    """An array of 32-bit signed integers each representing the day since epoch"""

    def __init__(self, values: List[Optional[datetime.date]]):
        def _transform(value: Optional[datetime.date]):
            if value is None:
                return None
            else:
                return (value - _DATE_EPOCH).days

        values = list(map(_transform, values))
        self._array = _arrowdantic_internal.Int32Array.from_date(values)

    def __iter__(self):
        return DateIterator(self._array.__iter__())


class DateIterator:
    __slots__ = ("_iter")

    def __init__(self, iter):
        self._iter = iter

    def __iter__(self):
        return self

    def __next__(self):
        dt_i32 = next(self._iter)
        if dt_i32:
            return _DATE_EPOCH + datetime.timedelta(days=dt_i32)



class TimeArray(Int64Array):
    """An array of 64-bit signed integers each representing the naive time since midnight with microsecond precision"""

    def __init__(self, values: List[Optional[datetime.time]]):
        def _transform(value: Optional[datetime.time]):
            if value is None:
                return None
            else:
                return ((value.hour * 60 + value.minute) * 60 + value.second) * 10**6 + value.microsecond

        values = list(map(_transform, values))
        self._array = _arrowdantic_internal.Int64Array.from_time_us(values)

    def __iter__(self):
        return TimeIterator(self._array.__iter__())


class TimeIterator:
    __slots__ = ("_iter")

    def __init__(self, iter):
        self._iter = iter

    def __iter__(self):
        return self

    def __next__(self):
        time_us = next(self._iter)
        if time_us:
            return datetime.datetime.fromtimestamp(time_us / 10**6).time()



class Float32Array(Array):
    """An array of 32-bit floating point"""

    def __init__(self, values: List[Optional[float]]):
        self._array = _arrowdantic_internal.Float32Array(values)


class Float64Array(Array):
    """An array of 64-bit floating point"""

    def __init__(self, values: List[Optional[float]]):
        self._array = _arrowdantic_internal.Float64Array(values)


class UInt8Array(Array):
    """An array of 8-bit unsigned integers (also known as bytes)"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.UInt8Array(values)


class UInt16Array(Array):
    """An array of 16-bit unsigned integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.UInt16Array(values)


class UInt32Array(Array):
    """An array of 32-bit unsigned integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.UInt32Array(values)


class UInt64Array(Array):
    """An array of 64-bit unsigned integers"""

    def __init__(self, values: List[Optional[int]]):
        self._array = _arrowdantic_internal.UInt64Array(values)


class BooleanArray(Array):
    """An array of booleans"""

    def __init__(self, values: List[Optional[bool]]):
        self._array = _arrowdantic_internal.BooleanArray(values)


class StringArray(Array):
    """An array of strings"""

    def __init__(self, values: List[Optional[str]]):
        self._array = _arrowdantic_internal.StringArray(values)


class LargeStringArray(Array):
    """An array of strings. It differs from ``StringArray`` in that it can contain
    2x more items (and uses 2x more memory)"""

    def __init__(self, values: List[Optional[str]]):
        self._array = _arrowdantic_internal.LargeStringArray(values)


class BinaryArray(Array):
    """An array of (multiple) bytes per element."""

    def __init__(self, values: List[Optional[bytes]]):
        self._array = _arrowdantic_internal.BinaryArray(values)


class LargeBinaryArray(Array):
    """An array of (multiple) bytes per element. It differs from ``BinaryArray`` in
    that it can contain 2x more items (and uses 2x more memory)"""

    def __init__(self, values: List[Optional[bytes]]):
        self._array = _arrowdantic_internal.LargeBinaryArray(values)


class Chunk:
    """A list of ``Array``s all with the same length"""

    def __init__(self, arrays: List[Array]):
        self._chunk = _arrowdantic_internal.Chunk([x._array for x in arrays])

    @staticmethod
    def _from_chunk(chunk: _arrowdantic_internal.Chunk) -> "Chunk":
        a = Chunk([])
        a._chunk = chunk
        return a

    def arrays(self) -> List[Array]:
        """Returns the arrays - they are guaranteed to have the same length"""
        return [Array._from_array(array) for array in self._chunk.arrays()]

    def __repr__(self):
        return self._chunk.__repr__()

    def __len__(self) -> int:
        return self._chunk.__len__()


class ArrowFileReader:
    """
    An iterator of ``Chunk``, each corresponding to a record batch from an Arrow IPC file.
    Use this class to read Arrow IPC files.
    """

    def __init__(self, path_or_obj):
        self._reader = _arrowdantic_internal.ArrowFileReader(path_or_obj)

    def schema(self) -> Schema:
        schema = Schema()
        schema._schema = self._reader.schema()
        return schema

    def __iter__(self):
        return self

    def __next__(self):
        return Chunk._from_chunk(next(self._reader))


class ArrowFileWriter:
    """
    Context manager to write an Arrow IPC file. A file is composed by:

    * a header, written when the context manager is entered
    * multiple record batches, written via ``write``
    * a footer, written when the context manager exits
    """

    __slots__ = ("_writer", "_schema", "_path")

    def __init__(self, path_or_obj, schema: Schema):
        self._path = path_or_obj
        self._schema = schema
        self._writer = None

    def __enter__(self) -> "ArrowFileWriter":
        self._writer = _arrowdantic_internal.ArrowFileWriter(
            self._path, self._schema._schema
        )
        return self

    def write(self, chunk: Chunk):
        """
        Writes a ``Chunk`` into the file.
        """
        self._writer.write(chunk._chunk)

    def __exit__(self, _, __, ___):
        self._writer.__exit__()
        self._writer = None


class ParquetFileReader:
    """
    An iterator of ``Chunk`` from row groups of a Parquet file.
    """

    def __init__(self, path_or_obj):
        self._reader = _arrowdantic_internal.ParquetFileReader(path_or_obj)

    def schema(self) -> Schema:
        schema = Schema()
        schema._schema = self._reader.schema()
        return schema

    def __iter__(self):
        return self

    def __next__(self):
        return Chunk._from_chunk(next(self._reader))


class ParquetFileWriter:
    """
    Context manager to write a Parquet file. A file is composed by:

    * a header, written when the context manager is entered
    * multiple record batches, written via ``write``
    * a footer, written when the context manager exits
    """

    __slots__ = ("_writer", "_schema", "_path")

    def __init__(self, path_or_obj, schema: Schema):
        self._path = path_or_obj
        self._schema = schema
        self._writer = None

    def __enter__(self) -> "ParquetFileWriter":
        self._writer = _arrowdantic_internal.ParquetFileWriter(
            self._path, self._schema._schema
        )
        return self

    def write(self, chunk: Chunk):
        """
        Writes a ``Chunk`` into the file as a new row group
        """
        self._writer.write(chunk._chunk)

    def __exit__(self, _, __, ___):
        self._writer.__exit__()
        self._writer = None


class ODBCConnector:
    """
    Context manager to read and write an ODBC connection.
    """

    __slots__ = ("_connection", "_connection_string")

    def __init__(self, connection_string: str):
        self._connection_string = connection_string
        self._connection: Optional[_arrowdantic_internal.ODBCConnector] = None

    def __enter__(self) -> "ODBCConnector":
        self._connection = _arrowdantic_internal.ODBCConnector(self._connection_string)
        return self

    def execute(
        self, statement: str, batch_size: Optional[int] = None
    ) -> Optional[Iterable[Chunk]]:
        """
        Executes an SQL statement. When the statement is expected to return values, `batch_size` must
        be provided.
        """
        iterator = self._connection.execute(statement, batch_size)
        if iterator is None:
            return None
        else:
            return ODBCChunkIter._from_iter(iterator)
        return self._connection.execute(statement, batch_size)

    def write(self, statement: str, chunk: Chunk):
        """
        Writes a ``Chunk`` into the ODBC driver. The statement must have the same number
        of parameters as the number of arrays in `chunk`.

        Example: ``INSERT INTO table (c1, c2) VALUES (?, ?)`` with a chunk of 2 arrays.
        """
        self._connection.write(statement, chunk._chunk)

    def __exit__(self, _, __, ___):
        self._connection = None


class ODBCChunkIter:
    def _from_iter(iter: _arrowdantic_internal.ODBCIterator) -> "ODBCChunkIter":
        a = ODBCChunkIter()
        a._iter = iter
        return a

    def fields(self) -> List[Field]:
        return [Field._from_field(f) for f in self._iter.fields()]

    def __enter__(self) -> "ODBCChunkIter":
        return self

    def __exit__(self, _, __, ___):
        self._iter = None

    def __next__(self) -> Chunk:
        return Chunk._from_chunk(next(self._iter))


def _format_offset(off):
    # copied from std - it is private there :/
    s = ""
    if off is not None:
        if off.days < 0:
            sign = "-"
            off = -off
        else:
            sign = "+"
        hh, mm = divmod(off, datetime.timedelta(hours=1))
        mm, ss = divmod(mm, datetime.timedelta(minutes=1))
        s += "%s%02d:%02d" % (sign, hh, mm)
        if ss or ss.microseconds:
            s += ":%02d" % ss.seconds

            if ss.microseconds:
                s += ".%06d" % ss.microseconds
    return s
