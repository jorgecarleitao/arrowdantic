from typing import List, Optional, Union
import enum

import arrowdantic_internal


@enum.unique
class PhysicalType(enum.Enum):
    NULL = enum.auto()
    BOOL = enum.auto()
    UINT8 = enum.auto()
    UINT16 = enum.auto()
    UINT32 = enum.auto()
    UINT64 = enum.auto()
    INT8 = enum.auto()
    INT16 = enum.auto()
    INT32 = enum.auto()
    INT64 = enum.auto()
    FLOAT32 = enum.auto()
    FLOAT64 = enum.auto()
    STRING = enum.auto()
    LARGESTRING = enum.auto()
    BINARY = enum.auto()
    LARGEBINARY = enum.auto()
    FIXEDSIZEDBINARY = enum.auto()
    LIST = enum.auto()
    LARGELIST = enum.auto()
    FIXEDSIZEDLIST = enum.auto()
    STRUCT = enum.auto()


class LogicalType:
    def __init__(self, value: Union[type, str]):
        if value is int:
            dt = "int64"
        elif value is bool:
            dt = "bool"
        elif isinstance(value, str):
            dt = value
        else:
            dt = None
        self._dt = arrowdantic_internal.DataType(dt)

    def __repr__(self):
        return self._dt.__repr__()


class Array:
    def __repr__(self):
        return self._array.__repr__()

    def __len__(self) -> int:
        return self._array.__len__()


class Int32Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.Int32Array(values)


class UInt32Array(Array):
    def __init__(self, values: List[Optional[int]]):
        self._array = arrowdantic_internal.UInt32Array(values)


class BooleanArray(Array):
    def __init__(self, values: List[Optional[bool]]):
        self._array = arrowdantic_internal.BooleanArray(values)


class Chunk:
    def __init__(self, arrays: List[Array]):
        self._chunk = None

    @staticmethod
    def _from_chunk(chunk: arrowdantic_internal.Chunk) -> "Chunk":
        a = Chunk([])
        a._chunk = chunk
        return a

    def __repr__(self):
        return self._chunk.__repr__()

    def __len__(self) -> int:
        return self._chunk.__len__()


class FileReader:
    """
    An iterator of ``Chunk``, each corresponding to a group of arrays from the IPC file.
    """
    def __init__(self, path_or_obj):
        self._reader = arrowdantic_internal.FileReader(path_or_obj)

    def __iter__(self):
        return self

    def __next__(self):
        return Chunk._from_chunk(self._reader.__next__())
