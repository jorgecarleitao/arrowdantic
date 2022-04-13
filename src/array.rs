use arrow2::{
    array::{
        Array, BinaryArray as _BinaryArray, BooleanArray as _BooleanArray, PrimitiveArray,
        Utf8Array,
    },
    datatypes::PhysicalType,
};

use pyo3::class::basic::CompareOp;
use pyo3::prelude::*;

macro_rules! native {
    ($name:ident, $type:ty) => {
        #[derive(Clone, PartialEq, Debug)]
        #[pyclass]
        pub struct $name(PrimitiveArray<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Vec<$type>>() {
                    Ok(Self(PrimitiveArray::<$type>::from_vec(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<$type>>>() {
                    Ok(Self(PrimitiveArray::<$type>::from(values)))
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

native!(UInt8Array, u8);
native!(UInt16Array, u16);
native!(UInt32Array, u32);
native!(UInt64Array, u64);
native!(Int8Array, i8);
native!(Int16Array, i16);
native!(Int32Array, i32);
native!(Int64Array, i64);
native!(Float32Array, f32);
native!(Float64Array, f64);

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct BooleanArray(_BooleanArray);

#[pymethods]
impl BooleanArray {
    #[new]
    fn new(values: &PyAny) -> PyResult<Self> {
        Ok(if let Ok(values) = values.extract::<Vec<bool>>() {
            Self(_BooleanArray::from_slice(values))
        } else if let Ok(values) = values.extract::<Vec<Option<bool>>>() {
            Self(_BooleanArray::from(values))
        } else {
            todo!()
        })
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

macro_rules! primitive {
    ($array:expr, $py:expr,$type:ty, $local:ident) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .unwrap();
        $local(array.clone()).into_py($py)
    }};
}

macro_rules! binary {
    ($name:ident, $type:ty) => {
        #[derive(Clone, PartialEq, Debug)]
        #[pyclass]
        pub struct $name(_BinaryArray<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Vec<&[u8]>>() {
                    Ok(Self(_BinaryArray::<$type>::from_slice(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<&[u8]>>>() {
                    Ok(Self(_BinaryArray::<$type>::from(values)))
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

binary!(BinaryArray, i32);
binary!(LargeBinaryArray, i64);

macro_rules! string {
    ($name:ident, $type:ty) => {
        #[derive(Clone, PartialEq, Debug)]
        #[pyclass]
        pub struct $name(Utf8Array<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Vec<&str>>() {
                    Ok(Self(Utf8Array::<$type>::from_slice(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<&str>>>() {
                    Ok(Self(Utf8Array::<$type>::from(values)))
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

string!(StringArray, i32);
string!(LargeStringArray, i64);

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
