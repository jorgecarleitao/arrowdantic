use arrow2::array::{
    Array, BinaryArray as _BinaryArray, BooleanArray as _BooleanArray, PrimitiveArray, Utf8Array,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::array::*;

#[derive(Clone)]
#[pyclass]
pub struct BooleanIterator(_BooleanArray, usize);

#[pymethods]
impl BooleanIterator {
    #[new]
    pub fn new(array: PyRef<BooleanArray>) -> Self {
        Self(array.0.clone(), 0)
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<Option<bool>> {
        let index = slf.1;
        let array = &slf.0;
        if index < array.len() {
            let r = Some(if array.is_valid(index) {
                Some(array.value(index))
            } else {
                None
            });
            slf.1 += 1;
            r
        } else {
            None
        }
    }
}

macro_rules! primitive {
    ($array:ident, $name:ident, $type:ty) => {
        #[derive(Clone)]
        #[pyclass]
        pub struct $name(PrimitiveArray<$type>, usize);

        #[pymethods]
        impl $name {
            #[new]
            pub fn new(array: PyRef<$array>) -> Self {
                Self(array.0.clone(), 0)
            }

            fn __next__(mut slf: PyRefMut<Self>) -> Option<Option<$type>> {
                let index = slf.1;
                let array = &slf.0;
                if index < array.len() {
                    let r = Some(if array.is_valid(index) {
                        Some(array.value(index))
                    } else {
                        None
                    });
                    slf.1 += 1;
                    r
                } else {
                    None
                }
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
primitive!(Int32Array, Int32Iterator, i32);
primitive!(Int64Array, Int64Iterator, i64);
primitive!(Float32Array, Float32Iterator, f32);
primitive!(Float64Array, Float64Iterator, f64);

macro_rules! string {
    ($array:ident, $name:ident, $type:ty) => {
        #[derive(Clone)]
        #[pyclass]
        pub struct $name(Utf8Array<$type>, usize);

        #[pymethods]
        impl $name {
            #[new]
            pub fn new(array: PyRef<$array>) -> Self {
                Self(array.0.clone(), 0)
            }

            fn __next__(mut slf: PyRefMut<Self>) -> Option<Option<String>> {
                let index = slf.1;
                let array = &slf.0;
                if index < array.len() {
                    let r = Some(if array.is_valid(index) {
                        Some(array.value(index).to_string())
                    } else {
                        None
                    });
                    slf.1 += 1;
                    r
                } else {
                    None
                }
            }
        }
    };
}

string!(StringArray, StringIterator, i32);
string!(LargeStringArray, LargeStringIterator, i64);

macro_rules! binary {
    ($array:ident, $name:ident, $type:ty) => {
        #[derive(Clone)]
        #[pyclass]
        pub struct $name(_BinaryArray<$type>, usize);

        #[pymethods]
        impl $name {
            #[new]
            pub fn new(array: PyRef<$array>) -> Self {
                Self(array.0.clone(), 0)
            }

            fn __next__(mut slf: PyRefMut<Self>) -> Option<Option<PyObject>> {
                let index = slf.1;
                let array = &slf.0;
                if index < array.len() {
                    let r = Some(if array.is_valid(index) {
                        Python::with_gil(|py| {
                            Some(array.value(index)).map(|r| PyBytes::new(py, r).into())
                        })
                    } else {
                        None
                    });
                    slf.1 += 1;
                    r
                } else {
                    None
                }
            }
        }
    };
}

binary!(BinaryArray, BinaryIterator, i32);
binary!(LargeBinaryArray, LargeBinaryIterator, i64);
