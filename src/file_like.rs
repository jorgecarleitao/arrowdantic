use std::fs::File;
use std::io::{BufReader, Read, Seek};

use pyo3::prelude::*;
use pyo3::types::PyString;

use super::py_file::PyFileLikeObject;

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileOrFileLike {
    File(BufReader<File>),
    FileLike(PyFileLikeObject),
}

impl Seek for FileOrFileLike {
    #[inline]
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(file) => file.seek(pos),
            Self::FileLike(file) => file.seek(pos),
        }
    }
}

impl Read for FileOrFileLike {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buf),
            Self::FileLike(file) => file.read(buf),
        }
    }
}

impl FileOrFileLike {
    pub fn from_pyobject(path_or_file_like: PyObject) -> PyResult<FileOrFileLike> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        // is a path
        if let Ok(string_ref) = path_or_file_like.cast_as::<PyString>(py) {
            let path = string_ref.to_string_lossy().to_string();
            return Ok(FileOrFileLike::File(BufReader::new(File::open(path)?)));
        }

        // is a file-like
        match PyFileLikeObject::with_requirements(path_or_file_like, true, false, true) {
            Ok(f) => Ok(FileOrFileLike::FileLike(f)),
            Err(e) => Err(e),
        }
    }
}
