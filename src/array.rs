use arrow2::{
    array::{
        Array, BinaryArray as _BinaryArray, BooleanArray as _BooleanArray, PrimitiveArray,
        Utf8Array,
    },
    datatypes::{DataType, PhysicalType, TimeUnit},
};

use pyo3::prelude::*;
use pyo3::types::PyIterator;
use pyo3::{class::basic::CompareOp, types::PyType};

use super::datatypes;
use super::iterator;

macro_rules! primitive {
    ($name:ident, $iterator:ident, $type:ty) => {
        #[derive(Clone, PartialEq, Debug)]
        #[pyclass]
        pub struct $name(pub PrimitiveArray<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Self>() {
                    Ok(values)
                } else if let Ok(values) = values.extract::<Vec<$type>>() {
                    Ok(Self(PrimitiveArray::<$type>::from_vec(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<$type>>>() {
                    Ok(Self(PrimitiveArray::<$type>::from(values)))
                } else if let Ok(values) = values.extract::<&PyIterator>() {
                    values
                        .map(|x| x.and_then(|x| x.extract::<Option<$type>>()))
                        .collect::<Result<PrimitiveArray<$type>, _>>()
                        .map(Self)
                } else {
                    todo!()
                }
            }

            fn __repr__(&self) -> String {
                format!("{:?}", &self.0 as &dyn Array)
            }

            fn __str__(&self) -> String {
                self.__repr__()
            }

            fn __len__(&self) -> usize {
                self.0.len()
            }

            fn __iter__(slf: PyRef<Self>) -> iterator::$iterator {
                iterator::$iterator::new(slf)
            }

            #[getter(type)]
            fn dtype(&self) -> datatypes::DataType {
                datatypes::DataType(self.0.data_type().clone())
            }

            fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
                Ok(if let Ok(other) = other.extract::<$name>(py) {
                    match op {
                        CompareOp::Eq => self.0 == other.0,
                        CompareOp::Ne => self.0 != other.0,
                        _ => todo!(),
                    }
                } else {
                    false
                })
            }
        }
    };
}

primitive!(UInt8Array, UInt8Iterator, u8);
primitive!(UInt16Array, UInt16Iterator, u16);
primitive!(UInt32Array, UInt32Iterator, u32);
primitive!(UInt64Array, UInt64Iterator, u64);
primitive!(Int8Array, Int8Iterator, i8);
primitive!(Int16Array, Int16Iterator, i16);
//primitive!(Int32Array, Int32Iterator, i32);
//primitive!(Int64Array, Int64Iterator, i64);
primitive!(Float32Array, Float32Iterator, f32);
primitive!(Float64Array, Float64Iterator, f64);

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct Int64Array(pub PrimitiveArray<i64>);

#[pymethods]
impl Int64Array {
    #[new]
    fn new(values: &PyAny) -> PyResult<Self> {
        if let Ok(values) = values.extract::<Self>() {
            Ok(values)
        } else if let Ok(values) = values.extract::<Vec<i64>>() {
            Ok(Self(PrimitiveArray::<i64>::from_vec(values)))
        } else if let Ok(values) = values.extract::<Vec<Option<i64>>>() {
            Ok(Self(PrimitiveArray::<i64>::from(values)))
        } else if let Ok(values) = values.extract::<&PyIterator>() {
            values
                .map(|x| x.and_then(|x| x.extract::<Option<i64>>()))
                .collect::<Result<PrimitiveArray<i64>, _>>()
                .map(Self)
        } else {
            todo!()
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", &self.0 as &dyn Array)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __iter__(slf: PyRef<Self>) -> iterator::Int64Iterator {
        iterator::Int64Iterator::new(slf)
    }

    #[getter(type)]
    fn dtype(&self) -> datatypes::DataType {
        datatypes::DataType(self.0.data_type().clone())
    }

    fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
        Ok(if let Ok(other) = other.extract::<Int64Array>(py) {
            match op {
                CompareOp::Eq => self.0 == other.0,
                CompareOp::Ne => self.0 != other.0,
                _ => todo!(),
            }
        } else {
            false
        })
    }

    #[classmethod]
    fn from_ts_s(_: &PyType, values: &PyAny, tz: Option<String>) -> PyResult<Self> {
        Self::new(values)
            .map(|v| v.0.to(DataType::Timestamp(TimeUnit::Second, tz)))
            .map(Self)
    }

    #[classmethod]
    fn from_ts_ms(_: &PyType, values: &PyAny, tz: Option<String>) -> PyResult<Self> {
        Self::new(values)
            .map(|v| v.0.to(DataType::Timestamp(TimeUnit::Millisecond, tz)))
            .map(Self)
    }

    #[classmethod]
    fn from_ts_us(_: &PyType, values: &PyAny, tz: Option<String>) -> PyResult<Self> {
        Self::new(values)
            .map(|v| v.0.to(DataType::Timestamp(TimeUnit::Microsecond, tz)))
            .map(Self)
    }

    #[classmethod]
    fn from_ts_ns(_: &PyType, values: &PyAny, tz: Option<String>) -> PyResult<Self> {
        Self::new(values)
            .map(|v| v.0.to(DataType::Timestamp(TimeUnit::Nanosecond, tz)))
            .map(Self)
    }

    #[classmethod]
    fn from_time_us(_: &PyType, values: &PyAny) -> PyResult<Self> {
        let values = Self::new(values)?;
        Ok(Self(values.0.to(DataType::Time64(TimeUnit::Microsecond))))
    }
}

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct Int32Array(pub PrimitiveArray<i32>);

#[pymethods]
impl Int32Array {
    #[new]
    fn new(values: &PyAny) -> PyResult<Self> {
        if let Ok(values) = values.extract::<Self>() {
            Ok(values)
        } else if let Ok(values) = values.extract::<Vec<i32>>() {
            Ok(Self(PrimitiveArray::<i32>::from_vec(values)))
        } else if let Ok(values) = values.extract::<Vec<Option<i32>>>() {
            Ok(Self(PrimitiveArray::<i32>::from(values)))
        } else if let Ok(values) = values.extract::<&PyIterator>() {
            values
                .map(|x| x.and_then(|x| x.extract::<Option<i32>>()))
                .collect::<Result<PrimitiveArray<i32>, _>>()
                .map(Self)
        } else {
            todo!()
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", &self.0 as &dyn Array)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __iter__(slf: PyRef<Self>) -> iterator::Int32Iterator {
        iterator::Int32Iterator::new(slf)
    }

    #[getter(type)]
    fn dtype(&self) -> datatypes::DataType {
        datatypes::DataType(self.0.data_type().clone())
    }

    fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
        Ok(if let Ok(other) = other.extract::<Int32Array>(py) {
            match op {
                CompareOp::Eq => self.0 == other.0,
                CompareOp::Ne => self.0 != other.0,
                _ => todo!(),
            }
        } else {
            false
        })
    }

    #[classmethod]
    fn from_date(_: &PyType, values: &PyAny) -> PyResult<Self> {
        let values = Self::new(values)?;
        Ok(Self(values.0.to(DataType::Date32)))
    }
}

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct BooleanArray(pub _BooleanArray);

#[pymethods]
impl BooleanArray {
    #[new]
    fn new(values: &PyAny) -> PyResult<Self> {
        if let Ok(values) = values.extract::<BooleanArray>() {
            Ok(values)
        } else if let Ok(values) = values.extract::<Vec<bool>>() {
            Ok(Self(_BooleanArray::from_slice(values)))
        } else if let Ok(values) = values.extract::<Vec<Option<bool>>>() {
            Ok(Self(_BooleanArray::from(values)))
        } else if let Ok(values) = values.extract::<&PyIterator>() {
            values
                .map(|x| x.and_then(|x| x.extract::<Option<bool>>()))
                .collect::<Result<_BooleanArray, _>>()
                .map(Self)
        } else {
            todo!()
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", &self.0 as &dyn Array)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    #[getter(type)]
    fn dtype(&self) -> datatypes::DataType {
        datatypes::DataType(self.0.data_type().clone())
    }

    fn __iter__(slf: PyRef<Self>) -> iterator::BooleanIterator {
        iterator::BooleanIterator::new(slf)
    }

    fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
        Ok(if let Ok(other) = other.extract::<BooleanArray>(py) {
            match op {
                CompareOp::Eq => self.0 == other.0,
                CompareOp::Ne => self.0 != other.0,
                _ => todo!(),
            }
        } else {
            false
        })
    }
}

macro_rules! binary {
    ($name:ident, $iterator:ident, $type:ty) => {
        #[derive(Clone, PartialEq, Debug)]
        #[pyclass]
        pub struct $name(pub _BinaryArray<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Self>() {
                    Ok(values)
                } else if let Ok(values) = values.extract::<Vec<&[u8]>>() {
                    Ok(Self(_BinaryArray::<$type>::from_slice(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<&[u8]>>>() {
                    Ok(Self(_BinaryArray::<$type>::from(values)))
                } else if let Ok(values) = values.extract::<&PyIterator>() {
                    values
                        .map(|x| x.and_then(|x| x.extract::<Option<&[u8]>>()))
                        .collect::<Result<_BinaryArray<$type>, _>>()
                        .map(Self)
                } else {
                    todo!()
                }
            }

            fn __repr__(&self) -> String {
                format!("{:?}", &self.0 as &dyn Array)
            }

            fn __str__(&self) -> String {
                self.__repr__()
            }

            fn __len__(&self) -> usize {
                self.0.len()
            }

            #[getter(type)]
            fn dtype(&self) -> datatypes::DataType {
                datatypes::DataType(self.0.data_type().clone())
            }

            fn __iter__(slf: PyRef<Self>) -> iterator::$iterator {
                iterator::$iterator::new(slf)
            }

            fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
                Ok(if let Ok(other) = other.extract::<$name>(py) {
                    match op {
                        CompareOp::Eq => self.0 == other.0,
                        CompareOp::Ne => self.0 != other.0,
                        _ => todo!(),
                    }
                } else {
                    false
                })
            }
        }
    };
}

binary!(BinaryArray, BinaryIterator, i32);
binary!(LargeBinaryArray, LargeBinaryIterator, i64);

macro_rules! string {
    ($name:ident, $iterator:ident, $type:ty) => {
        #[derive(Clone, PartialEq, Debug)]
        #[pyclass]
        pub struct $name(pub Utf8Array<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Self>() {
                    Ok(values)
                } else if let Ok(values) = values.extract::<Vec<&str>>() {
                    Ok(Self(Utf8Array::<$type>::from_slice(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<&str>>>() {
                    Ok(Self(Utf8Array::<$type>::from(values)))
                } else if let Ok(values) = values.extract::<&PyIterator>() {
                    values
                        .map(|x| x.and_then(|x| x.extract::<Option<&str>>()))
                        .collect::<Result<Utf8Array<$type>, _>>()
                        .map(Self)
                } else {
                    todo!()
                }
            }

            fn __repr__(&self) -> String {
                format!("{:?}", &self.0 as &dyn Array)
            }

            fn __str__(&self) -> String {
                self.__repr__()
            }

            fn __len__(&self) -> usize {
                self.0.len()
            }

            fn __iter__(slf: PyRef<Self>) -> iterator::$iterator {
                iterator::$iterator::new(slf)
            }

            #[getter(type)]
            fn dtype(&self) -> datatypes::DataType {
                datatypes::DataType(self.0.data_type().clone())
            }

            fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
                Ok(if let Ok(other) = other.extract::<$name>(py) {
                    match op {
                        CompareOp::Eq => self.0 == other.0,
                        CompareOp::Ne => self.0 != other.0,
                        _ => todo!(),
                    }
                } else {
                    false
                })
            }
        }
    };
}

string!(StringArray, StringIterator, i32);
string!(LargeStringArray, LargeStringIterator, i64);

macro_rules! primitive {
    ($array:expr, $py:expr,$type:ty, $local:ident) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .unwrap();
        $local(array.clone()).into_py($py)
    }};
}

pub fn to_py_object(py: Python, array: &dyn Array) -> PyObject {
    use arrow2::datatypes::PrimitiveType::*;
    match array.data_type().to_physical_type() {
        PhysicalType::Boolean => {
            let array = array.as_any().downcast_ref::<_BooleanArray>().unwrap();
            BooleanArray(array.clone()).into_py(py)
        }
        PhysicalType::Primitive(primitive) => match primitive {
            Int8 => primitive!(array, py, i8, Int8Array),
            Int16 => primitive!(array, py, i16, Int16Array),
            Int32 => primitive!(array, py, i32, Int32Array),
            Int64 => primitive!(array, py, i64, Int64Array),
            Int128 => todo!(),
            UInt8 => primitive!(array, py, u8, UInt8Array),
            UInt16 => primitive!(array, py, u16, UInt16Array),
            UInt32 => primitive!(array, py, u32, UInt32Array),
            UInt64 => primitive!(array, py, u64, UInt64Array),
            Float16 => todo!(),
            Float32 => primitive!(array, py, f32, Float32Array),
            Float64 => primitive!(array, py, f64, Float64Array),
            DaysMs => todo!(),
            MonthDayNano => todo!(),
        },
        PhysicalType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            StringArray(array.clone()).into_py(py)
        }
        PhysicalType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            LargeStringArray(array.clone()).into_py(py)
        }
        PhysicalType::Binary => {
            let array = array.as_any().downcast_ref::<_BinaryArray<i32>>().unwrap();
            BinaryArray(array.clone()).into_py(py)
        }
        PhysicalType::LargeBinary => {
            let array = array.as_any().downcast_ref::<_BinaryArray<i64>>().unwrap();
            LargeBinaryArray(array.clone()).into_py(py)
        }
        _ => todo!(),
    }
}

pub fn from_py_object(py: Python, array: PyObject) -> Box<dyn Array> {
    if let Ok(array) = array.extract::<BooleanArray>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<Int8Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<Int16Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<Int32Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<Int64Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<UInt8Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<UInt16Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<UInt32Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<UInt64Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<Float32Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<Float64Array>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<StringArray>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<LargeStringArray>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<BinaryArray>(py) {
        array.0.boxed()
    } else if let Ok(array) = array.extract::<LargeBinaryArray>(py) {
        array.0.boxed()
    } else {
        todo!("{:?}", array)
    }
}
