mod error;
mod file_like;
mod py_file;

use std::sync::Arc;

use pyo3::prelude::*;

use arrow2::array::{Array, BooleanArray as _BooleanArray, PrimitiveArray as _PrimitiveArray};
use arrow2::chunk::Chunk as _Chunk;
use arrow2::datatypes::{DataType as _DataType, Field as _Field};
use arrow2::io::ipc::read;

use error::Error;

#[pyclass]
struct DataType(_DataType);

#[pymethods]
impl DataType {
    #[new]
    fn new(type_: &PyAny) -> Self {
        if let Ok(type_) = type_.extract::<String>() {
            match type_.as_ref() {
                "bool" => Self(_DataType::Boolean),
                "int32" => Self(_DataType::Int32),
                "int64" => Self(_DataType::Int64),
                _ => todo!(),
            }
        } else {
            todo!()
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[pyclass]
struct Field(_Field);

macro_rules! native {
    ($name:ident, $type:ty) => {
        #[pyclass]
        struct $name(_PrimitiveArray<$type>);

        #[pymethods]
        impl $name {
            #[new]
            fn new(values: &PyAny) -> PyResult<Self> {
                if let Ok(values) = values.extract::<Vec<$type>>() {
                    Ok(Self(_PrimitiveArray::<$type>::from_vec(values)))
                } else if let Ok(values) = values.extract::<Vec<Option<$type>>>() {
                    Ok(Self(_PrimitiveArray::<$type>::from(values)))
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
        }
    };
}

native!(UInt32Array, u32);
native!(Int32Array, i32);
native!(Int64Array, i64);

#[pyclass]
struct BooleanArray(_BooleanArray);

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
}

#[pyclass]
struct Chunk(pub _Chunk<Arc<dyn Array>>);

#[pymethods]
impl Chunk {
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }
}

#[pyclass]
struct FileReader(read::FileReader<file_like::FileOrFileLike>);

#[pymethods]
impl FileReader {
    #[new]
    fn new(obj: PyObject) -> PyResult<Self> {
        let mut reader = file_like::FileOrFileLike::from_pyobject(obj)?;

        let metadata = read::read_file_metadata(&mut reader).map_err(Error)?;
        let reader = read::FileReader::new(reader, metadata, None);

        Ok(Self(reader))
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<Chunk>> {
        let chunk = slf.0.next().transpose().map_err(Error)?;
        Ok(chunk.map(Chunk))
    }
}

#[pymodule]
fn arrowdantic_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<UInt32Array>()?;
    m.add_class::<Chunk>()?;
    m.add_class::<FileReader>()?;
    m.add_class::<Int32Array>()?;
    m.add_class::<Int64Array>()?;
    m.add_class::<BooleanArray>()?;
    m.add_class::<DataType>()?;
    m.add_class::<Field>()?;
    Ok(())
}
