use pyo3::prelude::*;

use arrow2::chunk::Chunk as _Chunk;
use arrow2::datatypes::Field as _Field;
use arrow2::io::odbc;

use super::super::datatypes::Field;
use super::super::Chunk;
use super::super::Error;

static ENVIRONMENT: once_cell::sync::Lazy<odbc::api::Environment> =
    once_cell::sync::Lazy::new(|| odbc::api::Environment::new().unwrap());

#[pyclass(unsendable)]
pub struct ODBCConnector(odbc::api::Connection<'static>);

#[pymethods]
impl ODBCConnector {
    #[new]
    fn new(connection_string: String) -> PyResult<Self> {
        let connection = ENVIRONMENT
            .connect_with_connection_string(&connection_string)
            .map_err(arrow2::error::Error::from_external_error)
            .map_err(Error)?;
        Ok(Self(connection))
    }

    fn write(slf: PyRefMut<Self>, query: &str, chunk: PyRef<Chunk>) -> PyResult<()> {
        let prepared = slf
            .0
            .prepare(query)
            .map_err(arrow2::error::Error::from_external_error)
            .map_err(Error)?;

        let fields = chunk
            .0
            .arrays()
            .iter()
            .map(|array| _Field::new("unused", array.data_type().clone(), array.null_count() > 0))
            .collect::<Vec<_>>();

        let mut writer = odbc::write::Writer::try_new(prepared, fields).map_err(Error)?;

        writer.write(&chunk.0).map_err(Error)?;
        Ok(())
    }

    fn execute(
        slf: PyRef<Self>,
        query: &str,
        batch_size: Option<usize>,
    ) -> PyResult<Option<ODBCIterator>> {
        let maybe_cursor = odbc::read::execute(&slf.0, query, (), batch_size).map_err(Error)?;

        Ok(maybe_cursor.map(ODBCIterator))
    }
}

#[pyclass(unsendable)]
pub struct ODBCIterator(pub odbc::read::ChunkIterator<'static>);

#[pymethods]
impl ODBCIterator {
    fn fields(slf: PyRef<Self>) -> Vec<Field> {
        slf.0.fields().iter().cloned().map(Field).collect()
    }

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
