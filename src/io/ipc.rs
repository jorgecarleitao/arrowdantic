use pyo3::prelude::*;

use arrow2::io::ipc;

use super::super::datatypes::Schema;
use super::super::file_like;
use super::super::Chunk;
use super::super::Error;

#[pyclass]
pub struct ArrowFileReader(ipc::read::FileReader<file_like::FileReader>);

#[pymethods]
impl ArrowFileReader {
    #[new]
    fn new(obj: PyObject) -> PyResult<Self> {
        let mut reader = file_like::FileReader::from_pyobject(obj)?;

        let metadata = ipc::read::read_file_metadata(&mut reader).map_err(Error)?;
        let reader = ipc::read::FileReader::new(reader, metadata, None, None);

        Ok(Self(reader))
    }

    fn schema(slf: PyRef<Self>) -> Schema {
        Schema(slf.0.schema().clone())
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<Chunk>> {
        let chunk = slf.0.next().transpose().map_err(Error)?;
        Ok(chunk.map(Chunk))
    }
}

#[pyclass]
pub struct ArrowFileWriter(ipc::write::FileWriter<file_like::FileWriter>);

#[pymethods]
impl ArrowFileWriter {
    #[new]
    fn new(obj: PyObject, schema: Schema) -> PyResult<Self> {
        let writer = file_like::FileWriter::from_pyobject(obj)?;

        let reader = ipc::write::FileWriter::try_new(
            writer,
            &schema.0,
            None,
            ipc::write::WriteOptions { compression: None },
        )
        .map_err(Error)?;

        Ok(Self(reader))
    }

    fn write(mut slf: PyRefMut<Self>, chunk: PyRef<Chunk>) -> PyResult<()> {
        Ok(slf.0.write(&chunk.0, None).map_err(Error)?)
    }

    fn __enter__(slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf
    }

    fn __exit__(mut slf: PyRefMut<Self>) -> PyResult<()> {
        slf.0.finish().map_err(Error)?;
        Ok(())
    }
}
