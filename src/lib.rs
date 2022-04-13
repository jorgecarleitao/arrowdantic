mod array;
mod error;
mod file_like;
mod io;
mod py_file;

use std::sync::Arc;

use pyo3::prelude::*;

use arrow2::array::Array as _Array;
use arrow2::chunk::Chunk as _Chunk;
use arrow2::datatypes::{DataType as _DataType, Field as _Field};

use array::*;
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

#[pyclass]
struct Chunk(pub _Chunk<Arc<dyn _Array>>);

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

    fn arrays(&self, py: Python) -> Vec<PyObject> {
        self.0
            .arrays()
            .iter()
            .map(|x| to_py_object(py, x.as_ref()))
            .collect()
    }
}

#[pymodule]
fn arrowdantic_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Chunk>()?;

    m.add_class::<io::ArrowFileReader>()?;
    m.add_class::<io::ParquetFileReader>()?;

    m.add_class::<Int8Array>()?;
    m.add_class::<Int16Array>()?;
    m.add_class::<Int32Array>()?;
    m.add_class::<Int64Array>()?;

    m.add_class::<UInt8Array>()?;
    m.add_class::<UInt16Array>()?;
    m.add_class::<UInt32Array>()?;
    m.add_class::<UInt64Array>()?;

    m.add_class::<Float32Array>()?;
    m.add_class::<Float64Array>()?;

    m.add_class::<BooleanArray>()?;

    m.add_class::<StringArray>()?;
    m.add_class::<LargeStringArray>()?;
    m.add_class::<BinaryArray>()?;
    m.add_class::<LargeBinaryArray>()?;

    m.add_class::<DataType>()?;
    m.add_class::<Field>()?;
    Ok(())
}
