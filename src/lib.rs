mod array;
mod datatypes;
mod error;
mod file_like;
mod io;
mod iterator;
mod py_file;

use pyo3::prelude::*;

use arrow2::array::Array;
use arrow2::chunk::Chunk as _Chunk;

use array::*;
use error::Error;

#[pyclass]
struct Chunk(pub _Chunk<Box<dyn Array>>);

#[pymethods]
impl Chunk {
    #[new]
    fn new(py: Python, arrays: Vec<PyObject>) -> PyResult<Self> {
        let arrays = arrays
            .into_iter()
            .map(|array| from_py_object(py, array))
            .collect();
        Ok(_Chunk::try_new(arrays).map_err(Error).map(Self)?)
    }

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
fn arrowdantic(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Chunk>()?;

    m.add_class::<io::ArrowFileReader>()?;
    m.add_class::<io::ArrowFileWriter>()?;
    m.add_class::<io::ParquetFileReader>()?;
    m.add_class::<io::ParquetFileWriter>()?;
    m.add_class::<io::ODBCConnector>()?;
    m.add_class::<io::ODBCIterator>()?;

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

    m.add_class::<datatypes::DataType>()?;
    m.add_class::<datatypes::Field>()?;
    m.add_class::<datatypes::Schema>()?;
    Ok(())
}
