use pyo3::{exceptions::PyOSError, PyErr};

pub struct Error(pub arrow2::error::Error);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<arrow2::error::Error> for Error {
    fn from(err: arrow2::error::Error) -> Error {
        Error(err)
    }
}

impl std::convert::From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}
