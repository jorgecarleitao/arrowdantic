use pyo3::prelude::*;

use arrow2::chunk::Chunk as _Chunk;
use arrow2::datatypes::Field;
use arrow2::io::ipc;
use arrow2::io::odbc;
use arrow2::io::parquet;

use super::datatypes::Schema;
use super::file_like;
use super::Chunk;
use super::Error;

#[pyclass]
pub struct ArrowFileReader(ipc::read::FileReader<file_like::FileReader>);

#[pymethods]
impl ArrowFileReader {
    #[new]
    fn new(obj: PyObject) -> PyResult<Self> {
        let mut reader = file_like::FileReader::from_pyobject(obj)?;

        let metadata = ipc::read::read_file_metadata(&mut reader).map_err(Error)?;
        let reader = ipc::read::FileReader::new(reader, metadata, None);

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

#[pyclass]
pub struct ParquetFileReader(parquet::read::FileReader<file_like::FileReader>);

#[pymethods]
impl ParquetFileReader {
    #[new]
    fn new(obj: PyObject) -> PyResult<Self> {
        let reader = file_like::FileReader::from_pyobject(obj)?;

        let reader =
            parquet::read::FileReader::try_new(reader, None, None, None, None).map_err(Error)?;

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

static ENVIRONMENT: once_cell::sync::Lazy<odbc::api::Environment> =
    once_cell::sync::Lazy::new(|| odbc::api::Environment::new().unwrap());

#[pyclass(unsendable)]
pub struct ODBCWriter(odbc::api::Connection<'static>);

#[pymethods]
impl ODBCWriter {
    #[new]
    fn new(connection_string: String) -> PyResult<Self> {
        let connection = ENVIRONMENT
            .connect_with_connection_string(&connection_string)
            .map_err(arrow2::error::ArrowError::from_external_error)
            .map_err(Error)?;
        Ok(Self(connection))
    }

    fn write(slf: PyRefMut<Self>, query: &str, chunk: PyRef<Chunk>) -> PyResult<()> {
        let prepared = slf
            .0
            .prepare(query)
            .map_err(arrow2::error::ArrowError::from_external_error)
            .map_err(Error)?;

        let fields = chunk
            .0
            .arrays()
            .iter()
            .map(|array| Field::new("unused", array.data_type().clone(), array.null_count() > 0))
            .collect::<Vec<_>>();

        let mut writer = odbc::write::Writer::try_new(prepared, fields).map_err(Error)?;

        writer.write(&chunk.0).map_err(Error)?;
        Ok(())
    }

    fn execute(slf: PyRef<Self>, query: &str, batch_size: usize) -> PyResult<Option<ODBCIterator>> {
        let maybe_cursor = odbc::read::execute(&slf.0, query, (), batch_size).map_err(Error)?;

        Ok(maybe_cursor.map(ODBCIterator))
    }
}

#[pyclass(unsendable)]
pub struct ODBCIterator(pub odbc::read::ChunkIterator<'static>);

#[pymethods]
impl ODBCIterator {
    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<Chunk>> {
        Ok(slf
            .0
            .next()
            .transpose()
            .map_err(Error)?
            .map(|chunk| {
                chunk
                    .into_arrays()
                    .into_iter()
                    .map(|array| array.into())
                    .collect()
            })
            .map(_Chunk::new)
            .map(Chunk))
    }
}
