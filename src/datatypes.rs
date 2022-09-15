use pyo3::{prelude::*, pyclass::CompareOp, types::PyType};

use arrow2::datatypes::{DataType as _DataType, Field as _Field, Schema as _Schema, TimeUnit};

#[derive(Clone, PartialEq, Eq, Debug)]
#[pyclass]
pub struct DataType(pub _DataType);

#[pymethods]
impl DataType {
    #[classmethod]
    fn uint8(_: &PyType) -> Self {
        Self(_DataType::UInt8)
    }

    #[classmethod]
    fn uint16(_: &PyType) -> Self {
        Self(_DataType::UInt16)
    }

    #[classmethod]
    fn uint32(_: &PyType) -> Self {
        Self(_DataType::UInt32)
    }

    #[classmethod]
    fn uint64(_: &PyType) -> Self {
        Self(_DataType::UInt64)
    }

    #[classmethod]
    fn int8(_: &PyType) -> Self {
        Self(_DataType::Int8)
    }

    #[classmethod]
    fn int16(_: &PyType) -> Self {
        Self(_DataType::Int16)
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
    fn float32(_: &PyType) -> Self {
        Self(_DataType::Float32)
    }

    #[classmethod]
    fn float64(_: &PyType) -> Self {
        Self(_DataType::Float64)
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

    #[classmethod]
    fn ts_s(_: &PyType, tz: Option<String>) -> Self {
        Self(_DataType::Timestamp(TimeUnit::Second, tz))
    }

    #[classmethod]
    fn ts_ms(_: &PyType, tz: Option<String>) -> Self {
        Self(_DataType::Timestamp(TimeUnit::Millisecond, tz))
    }

    #[classmethod]
    fn ts_us(_: &PyType, tz: Option<String>) -> Self {
        Self(_DataType::Timestamp(TimeUnit::Microsecond, tz))
    }

    #[classmethod]
    fn ts_ns(_: &PyType, tz: Option<String>) -> Self {
        Self(_DataType::Timestamp(TimeUnit::Nanosecond, tz))
    }

    #[classmethod]
    fn date(_: &PyType) -> Self {
        Self(_DataType::Date32)
    }

    #[classmethod]
    fn time(_: &PyType) -> Self {
        Self(_DataType::Time64(TimeUnit::Microsecond))
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

    pub fn is_ts(&self) -> bool {
        matches!(&self.0, _DataType::Timestamp(_, _))
    }

    pub fn tz(&self) -> Option<String> {
        if let _DataType::Timestamp(_, tz) = &self.0 {
            tz.clone()
        } else {
            None
        }
    }

    pub fn timeunit(&self) -> Option<String> {
        if let _DataType::Timestamp(v, _) = &self.0 {
            match v {
                TimeUnit::Second => "s".to_string().into(),
                TimeUnit::Millisecond => "ms".to_string().into(),
                TimeUnit::Microsecond => "us".to_string().into(),
                TimeUnit::Nanosecond => "ns".to_string().into(),
            }
        } else {
            None
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
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

#[derive(Clone, PartialEq, Eq, Debug)]
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
