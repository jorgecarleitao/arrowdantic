"""
A library to read from and write to

* Apache Arrow IPC
* Apache Parquet
* ODBC (databases)
"""
import zoneinfo
import enum
import typing
import datetime
import abc

import arrowdantic.arrowdantic as _arrowdantic_internal


class TimeUnit(str, enum.Enum):
    """unit of representarion of a time"""

    s = "s"
    ms = "ms"
    us = "us"
    ns = "ns"


class DataType:
    """
    Arrow's representation of logical types.

    This class contains multiple class methods to initialize valid logical types
    """

    __slots__ = ("_dt",)

    @classmethod
    def bool(cls) -> "DataType":
        """Returns ``DataType`` representing boolean"""
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
    def float32(cls) -> "DataType":
        """Returns ``DataType`` representing a 32-bit float"""
        return cls._from_type(_arrowdantic_internal.DataType.float32())
    
    @classmethod
    def float64(cls) -> "DataType":
        """Returns ``DataType`` representing a 64-bit float"""
        return cls._from_type(_arrowdantic_internal.DataType.float64())

    @classmethod
    def string(cls) -> "DataType":
        """Returns ``DataType`` representing a string (utf8)"""
        return cls._from_type(_arrowdantic_internal.DataType.string())

    @classmethod
    def large_string(cls) -> "DataType":
        """Returns ``DataType`` representing a string (utf8)"""
        return cls._from_type(_arrowdantic_internal.DataType.large_string())

    @classmethod
    def binary(cls) -> "DataType":
        """Returns ``DataType`` representing binary (bytes)"""
        return cls._from_type(_arrowdantic_internal.DataType.binary())

    @classmethod
    def large_binary(cls) -> "DataType":
        """Returns ``DataType`` representing binary (bytes)"""
        return cls._from_type(_arrowdantic_internal.DataType.large_binary())

    @classmethod
    def timestamp(
        cls, unit: TimeUnit, tz: typing.Optional[datetime.tzinfo]
    ) -> "DataType":
        if tz:
            tz = tz.tzname(None)
        if unit == TimeUnit.s:
            return cls._from_type(_arrowdantic_internal.DataType.ts_s(tz))
        if unit == TimeUnit.ms:
            return cls._from_type(_arrowdantic_internal.DataType.ts_ms(tz))
        if unit == TimeUnit.us:
            return cls._from_type(_arrowdantic_internal.DataType.ts_us(tz))
        if unit == TimeUnit.ns:
            return cls._from_type(_arrowdantic_internal.DataType.ts_ns(tz))

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

    def __init__(self, fields: typing.List[Field]):
        self._schema = _arrowdantic_internal.Schema([f._field for f in fields])

    @property
    def fields(self):
        """The fields"""
        return [Field._from_field(f) for f in self._schema.fields]


class Array(abc.ABC):
    """An immutable, fixed length, Arrow-aligned sequence of typing.Optional elements.
    Different implementations represent different logical types (e.g. integers, booleans, strings)"""

    __slots__ = ("_array",)

    @classmethod
    def _from_array(cls, array):
        # dynamic dispatch of the array based to the corresponding types
        if array.type == _arrowdantic_internal.DataType.time():
            return TimeArray(array)
        if array.type == _arrowdantic_internal.DataType.date():
            return DateArray._from_array(array)
        if array.type.is_ts():
            return TimestampArray._from_array(array)
        if array.type == _arrowdantic_internal.DataType.bool():
            return BooleanArray(array)
        if array.type == _arrowdantic_internal.DataType.uint8():
            return UInt8Array(array)
        if array.type == _arrowdantic_internal.DataType.uint16():
            return UInt16Array(array)
        if array.type == _arrowdantic_internal.DataType.uint32():
            return UInt32Array(array)
        if array.type == _arrowdantic_internal.DataType.uint64():
            return UInt64Array(array)
        if array.type == _arrowdantic_internal.DataType.int8():
            return Int8Array(array)
        if array.type == _arrowdantic_internal.DataType.int16():
            return Int16Array(array)
        if array.type == _arrowdantic_internal.DataType.int32():
            return Int32Array(array)
        if array.type == _arrowdantic_internal.DataType.int64():
            return Int64Array(array)
        if array.type == _arrowdantic_internal.DataType.binary():
            return BinaryArray(array)
        if array.type == _arrowdantic_internal.DataType.large_binary():
            return LargeBinaryArray(array)
        if array.type == _arrowdantic_internal.DataType.string():
            return StringArray(array)
        if array.type == _arrowdantic_internal.DataType.large_string():
            return LargeStringArray(array)
        if array.type == _arrowdantic_internal.DataType.float32():
            return Float32Array(array)
        if array.type == _arrowdantic_internal.DataType.float64():
            return Float64Array(array)
        raise NotImplementedError(array.type)

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

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.Int8Array(values)


class Int16Array(Array):
    """An array of 16-bit signed integers"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.Int16Array(values)


class Int32Array(Array):
    """An array of 32-bit signed integers"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.Int32Array(values)


class Int64Array(Array):
    """An array of 64-bit signed integers"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.Int64Array(values)


class TimestampArray(Int64Array):
    """
    An array where each element represents a ``datetime`` with the same (typing.Optional) timezone

    Arrow timestamps are fundamentally incompatible with Python ``datetime``:

    * Arrow timestamps are represented as integers.
    * While Python ``datetime`` stores its timezone on a per-element, Arrow timestamps have
      a common timezone.
    """

    @classmethod
    def _from_array(cls, array):
        self = TimestampArray([], array.type.timeunit(), array.type.tz())
        self._array = array
        return self

    @property
    def tzinfo(self) -> typing.Optional[str]:
        """Returns the tzinfo of this array"""
        return self._array.type.tz()

    @property
    def timeunit(self) -> TimeUnit:
        """Returns the ``TimeUnit`` of this array"""
        return self._array.type.timeunit()

    def __init__(
        self, values: typing.List[typing.Optional[datetime.datetime]], unit: TimeUnit,
        tz: typing.Optional[datetime.tzinfo] = None,
    ):
        """Initializes a"""
        if not values:
            tz = None
            str_tz = None
        else:
            tz = values[0].tzinfo
            if tz is not None:
                str_tz = tz.tzname(values[0])
            else:
                str_tz = None

        if unit == TimeUnit.s:
            factor = 1
        if unit == TimeUnit.ms:
            factor = 10**3
        if unit == TimeUnit.us:
            factor = 10**6
        if unit == TimeUnit.ns:
            factor = 10**9

        def _transform(value: typing.Optional[datetime.datetime]):
            if value is None:
                return None
            else:
                if value.tzinfo != tz:
                    raise ValueError("Values must all have the same tzinfo")
                seconds = value.timestamp()
                microseconds = int(seconds * factor)
                return microseconds

        values = list(map(_transform, values))

        if unit == TimeUnit.s:
            self._array = _arrowdantic_internal.Int64Array.from_ts_s(values, str_tz)
        if unit == TimeUnit.ms:
            self._array = _arrowdantic_internal.Int64Array.from_ts_ms(values, str_tz)
        if unit == TimeUnit.us:
            self._array = _arrowdantic_internal.Int64Array.from_ts_us(values, str_tz)
        if unit == TimeUnit.ns:
            self._array = _arrowdantic_internal.Int64Array.from_ts_ns(values, str_tz)

    @classmethod
    def from_timestamps(
        cls,
        values: typing.Iterable[typing.Optional[int]],
        unit: TimeUnit,
        tz: typing.Optional[datetime.tzinfo],
    ) -> "TimestampArray":
        self = cls([], unit, tz)

        if tz is not None:
            str_tz = tz.tzname(None)
        else:
            str_tz = None
        if unit == TimeUnit.s:
            self._array = _arrowdantic_internal.Int64Array.from_ts_s(values, str_tz)
        elif unit == TimeUnit.ms:
            self._array = _arrowdantic_internal.Int64Array.from_ts_ms(values, str_tz)
        elif unit == TimeUnit.us:
            self._array = _arrowdantic_internal.Int64Array.from_ts_us(values, str_tz)
        elif unit == TimeUnit.ns:
            self._array = _arrowdantic_internal.Int64Array.from_ts_ns(values, str_tz)
        return self

    def __iter__(self) -> typing.Iterator[typing.Optional[datetime.datetime]]:
        return _TimestampIterator(self._array.__iter__(), self.timeunit, self.tzinfo)


class _TimestampIterator:
    """An iterator of timestamps"""

    __slots__ = ("_iter", "_tz", "_factor")

    def __init__(self, iter, unit: TimeUnit, tz: datetime.tzinfo):
        self._iter = iter
        self._tz = zoneinfo.ZoneInfo(tz)
        if unit == TimeUnit.s:
            factor = 1
        if unit == TimeUnit.ms:
            factor = 1 / 10**3
        if unit == TimeUnit.us:
            factor = 1 / 10**6
        if unit == TimeUnit.ns:
            factor = 1 / 10**9
        self._factor = factor

    def __iter__(self):
        return self

    def __next__(self) -> typing.Optional[datetime.datetime]:
        dt_i64 = next(self._iter)
        if dt_i64 is not None:
            return datetime.datetime.fromtimestamp(dt_i64 * self._factor, self._tz)


_DATE_EPOCH = datetime.datetime.utcfromtimestamp(0).date()


class DateArray(Int32Array):
    """An array of 32-bit signed integers each representing the day since epoch"""

    @classmethod
    def _from_array(cls, array):
        self = DateArray([])
        self._array = array

    def __init__(self, values: typing.List[typing.Optional[datetime.date]]):
        def _transform(value: typing.Optional[datetime.date]):
            if value is None:
                return None
            else:
                return (value - _DATE_EPOCH).days

        values = list(map(_transform, values))
        self._array = _arrowdantic_internal.Int32Array.from_date(values)

    def __iter__(self):
        return _DateIterator(self._array.__iter__())


class _DateIterator:
    __slots__ = "_iter"

    def __init__(self, iter):
        self._iter = iter

    def __iter__(self):
        return self

    def __next__(self):
        dt_i32 = next(self._iter)
        if dt_i32 is not None:
            return _DATE_EPOCH + datetime.timedelta(days=dt_i32)


class TimeArray(Int64Array):
    """An array of 64-bit signed integers each representing the naive time since midnight with microsecond precision"""

    def __init__(self, values: typing.List[typing.Optional[datetime.time]]):
        def _transform(value: typing.Optional[datetime.time]):
            if value is None:
                return None
            else:
                return (
                    (value.hour * 60 + value.minute) * 60 + value.second
                ) * 10**6 + value.microsecond

        values = list(map(_transform, values))
        self._array = _arrowdantic_internal.Int64Array.from_time_us(values)

    def __iter__(self):
        return _TimeIterator(self._array.__iter__())


class _TimeIterator:
    """An iterator of timestamps"""

    __slots__ = "_iter"

    def __init__(self, iter):
        self._iter = iter

    def __iter__(self):
        return self

    def __next__(self):
        time_us = next(self._iter)
        if time_us is not None:
            return datetime.datetime.fromtimestamp(time_us / 10**6).time()


class Float32Array(Array):
    """An array of 32-bit floating point"""

    def __init__(self, values: typing.Iterable[typing.Optional[float]]):
        self._array = _arrowdantic_internal.Float32Array(values)


class Float64Array(Array):
    """An array of 64-bit floating point"""

    def __init__(self, values: typing.Iterable[typing.Optional[float]]):
        self._array = _arrowdantic_internal.Float64Array(values)


class UInt8Array(Array):
    """An array of 8-bit unsigned integers (also known as bytes)"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.UInt8Array(values)


class UInt16Array(Array):
    """An array of 16-bit unsigned integers"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.UInt16Array(values)


class UInt32Array(Array):
    """An array of 32-bit unsigned integers"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.UInt32Array(values)


class UInt64Array(Array):
    """An array of 64-bit unsigned integers"""

    def __init__(self, values: typing.Iterable[typing.Optional[int]]):
        self._array = _arrowdantic_internal.UInt64Array(values)


class BooleanArray(Array):
    """An array of booleans"""

    def __init__(self, values: typing.Iterable[typing.Optional[bool]]):
        self._array = _arrowdantic_internal.BooleanArray(values)


class StringArray(Array):
    """An array of strings"""

    def __init__(self, values: typing.Iterable[typing.Optional[str]]):
        self._array = _arrowdantic_internal.StringArray(values)


class LargeStringArray(Array):
    """An array of strings. It differs from ``StringArray`` in that it can contain
    ~2^32 more items (and uses 2x more memory per item)"""

    def __init__(self, values: typing.Iterable[typing.Optional[str]]):
        self._array = _arrowdantic_internal.LargeStringArray(values)


class BinaryArray(Array):
    """An array of (multiple) bytes per element."""

    def __init__(self, values: typing.Iterable[typing.Optional[bytes]]):
        self._array = _arrowdantic_internal.BinaryArray(values)


class LargeBinaryArray(Array):
    """An array of (multiple) bytes per element. It differs from ``BinaryArray`` in
    that it can contain ~2^32 more items (and uses 2x more memory per item)"""

    def __init__(self, values: typing.Iterable[typing.Optional[bytes]]):
        self._array = _arrowdantic_internal.LargeBinaryArray(values)


class Chunk:
    """A list of ``Array``s all with the same length"""

    def __init__(self, arrays: typing.List[Array]):
        self._chunk = _arrowdantic_internal.Chunk([x._array for x in arrays])

    @staticmethod
    def _from_chunk(chunk: _arrowdantic_internal.Chunk) -> "Chunk":
        a = Chunk([])
        a._chunk = chunk
        return a

    def arrays(self) -> typing.List[Array]:
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

    The chunks are guaranteed to have the same schema.
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
    Use this class to read Parquet files.

    The chunks are guaranteed to have the same schema (provided by ``schema``).
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
        self._connection: typing.Optional[_arrowdantic_internal.ODBCConnector] = None

    def __enter__(self) -> "ODBCConnector":
        self._connection = _arrowdantic_internal.ODBCConnector(self._connection_string)
        return self

    def execute(
        self, statement: str, batch_size: typing.Optional[int] = None
    ) -> typing.Optional[typing.Iterable[Chunk]]:
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

    def fields(self) -> typing.List[Field]:
        return [Field._from_field(f) for f in self._iter.fields()]

    def __enter__(self) -> "ODBCChunkIter":
        return self

    def __exit__(self, _, __, ___):
        self._iter = None

    def __next__(self) -> Chunk:
        return Chunk._from_chunk(next(self._iter))
