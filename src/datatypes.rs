use pyo3::{prelude::*, pyclass::CompareOp, types::PyType};

use arrow2::datatypes::{DataType as _DataType, Field as _Field, Schema as _Schema};

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct DataType(pub _DataType);

#[pymethods]
impl DataType {
    #[classmethod]
    fn uint32(_: &PyType) -> Self {
        Self(_DataType::UInt32)
    }

    #[classmethod]
    fn int32(_: &PyType) -> Self {
        Self(_DataType::Int32)
    }

    #[classmethod]
    fn int64(_: &PyType) -> Self {
        Self(_DataType::Int64)
    }

    #[classmethod]
    fn bool(_: &PyType) -> Self {
        Self(_DataType::Boolean)
    }

    #[classmethod]
    fn string(_: &PyType) -> Self {
        Self(_DataType::Utf8)
    }

    #[classmethod]
    fn large_string(_: &PyType) -> Self {
        Self(_DataType::LargeUtf8)
    }

    #[classmethod]
    fn binary(_: &PyType) -> Self {
        Self(_DataType::Binary)
    }

    #[classmethod]
    fn large_binary(_: &PyType) -> Self {
        Self(_DataType::LargeBinary)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", &self.0)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
        Ok(if let Ok(other) = other.extract::<DataType>(py) {
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

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct Field(pub _Field);

#[pymethods]
impl Field {
    #[new]
    fn new(name: String, data_type: DataType, is_nullable: bool) -> Self {
        Self(_Field {
            name,
            data_type: data_type.0,
            is_nullable,
            metadata: Default::default(),
        })
    }

    #[getter(name)]
    fn name(&self) -> &str {
        &self.0.name
    }

    #[getter(nullable)]
    fn nullable(&self) -> bool {
        self.0.is_nullable
    }

    #[getter(data_type)]
    fn data_type(&self) -> DataType {
        DataType(self.0.data_type.clone())
    }

    fn __repr__(&self) -> String {
        format!("{:?}", &self.0)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __richcmp__(&self, py: Python, other: PyObject, op: CompareOp) -> PyResult<bool> {
        Ok(if let Ok(other) = other.extract::<Field>(py) {
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

#[derive(Clone, PartialEq, Debug)]
#[pyclass]
pub struct Schema(pub _Schema);

#[pymethods]
impl Schema {
    #[new]
    fn new(fields: Vec<PyRef<Field>>) -> Self {
        let fields = fields.into_iter().map(|field| field.0.clone()).collect();
        Self(_Schema {
            fields,
            metadata: Default::default(),
        })
    }

    #[getter(fields)]
    fn fields(&self) -> Vec<Field> {
        self.0.fields.iter().cloned().map(Field).collect()
    }
}
