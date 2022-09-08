use pyo3::prelude::*;

use arrow2::io::parquet;

use super::super::datatypes::Schema;
use super::super::file_like;
use super::super::Chunk;
use super::super::Error;

#[pyclass]
pub struct ParquetFileReader(parquet::read::FileReader<file_like::FileReader>, Schema);

#[pymethods]
impl ParquetFileReader {
    #[new]
    fn new(obj: PyObject) -> PyResult<Self> {
        let mut reader = file_like::FileReader::from_pyobject(obj)?;

        let metadata = parquet::read::read_metadata(&mut reader).map_err(Error)?;
        let schema = parquet::read::infer_schema(&metadata).map_err(Error)?;

        let reader = parquet::read::FileReader::new(
            reader,
            metadata.row_groups,
            schema.clone(),
            None,
            None,
            None,
        );

        let schema = Schema(schema);

        Ok(Self(reader, schema))
    }

    fn schema(slf: PyRef<Self>) -> Schema {
        slf.1.clone()
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
                compression: parquet::write::CompressionOptions::Uncompressed,
            },
        )
        .map_err(Error)?;

        Ok(Self(reader))
    }

    fn write(mut slf: PyRefMut<Self>, chunk: PyRef<Chunk>) -> PyResult<()> {
        let encodings = chunk
            .0
            .arrays()
            .iter()
            .map(|array| {
                parquet::write::transverse(array.data_type(), |_| parquet::write::Encoding::Plain)
            })
            .collect::<Vec<_>>();
        let fields = slf.0.parquet_schema().fields().to_vec();
        let row_group =
            parquet::write::row_group_iter(chunk.0.clone(), encodings, fields, slf.0.options());
        Ok(slf.0.write(row_group).map_err(Error)?)
    }

    fn __exit__(mut slf: PyRefMut<Self>) -> PyResult<()> {
        slf.0.end(None).map_err(Error)?;
        Ok(())
    }
}
