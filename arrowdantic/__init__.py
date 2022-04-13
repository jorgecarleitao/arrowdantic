from typing import List, Optional

import arrowdantic_internal


class Array:
    @classmethod
    def _from_array(cls, array):
        a = cls()
        a._array = array
        return a

    def __repr__(self):
        return self._array.__repr__()

    def __len__(self) -> int:
        return self._array.__len__()

    def __eq__(self, o: "Array") -> bool:
        return o._array == self._array


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
        self._chunk = None

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
