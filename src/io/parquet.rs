use pyo3::prelude::*;

use arrow2::io::parquet;

use super::super::datatypes::Schema;
use super::super::file_like;
use super::super::Chunk;
use super::super::Error;

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
pub struct ParquetFileWriter(parquet::write::FileWriter<file_like::FileWriter>);

#[pymethods]
impl ParquetFileWriter {
    #[new]
    fn new(obj: PyObject, schema: Schema) -> PyResult<Self> {
        let writer = file_like::FileWriter::from_pyobject(obj)?;

        let reader = parquet::write::FileWriter::try_new(
            writer,
            schema.0,
            parquet::write::WriteOptions {
                version: parquet::write::Version::V2,
                write_statistics: true,
                compression: parquet::write::Compression::Uncompressed,
            },
        )
        .map_err(Error)?;

        Ok(Self(reader))
    }

    fn write(mut slf: PyRefMut<Self>, chunk: PyRef<Chunk>) -> PyResult<()> {
        let encodings = vec![parquet::write::Encoding::Plain; chunk.0.arrays().len()];
        let descriptors = slf.0.parquet_schema().columns().to_vec();
        let row_group = parquet::write::row_group_iter(
            chunk.0.clone(),
            encodings,
            descriptors,
            slf.0.options(),
        );
        Ok(slf.0.write(row_group).map_err(Error)?)
    }

    fn __enter__(mut slf: PyRefMut<Self>) -> PyResult<PyRefMut<Self>> {
        slf.0.start().map_err(Error)?;
        Ok(slf)
    }

    fn __exit__(mut slf: PyRefMut<Self>) -> PyResult<()> {
        slf.0.end(None).map_err(Error)?;
        Ok(())
    }
}
