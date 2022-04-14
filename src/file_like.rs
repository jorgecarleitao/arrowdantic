use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};

use pyo3::prelude::*;
use pyo3::types::PyString;

use super::py_file::PyFileLikeObject;

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileReader {
    File(BufReader<File>),
    FileLike(PyFileLikeObject),
}

impl Seek for FileReader {
    #[inline]
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(file) => file.seek(pos),
            Self::FileLike(file) => file.seek(pos),
        }
    }
}

impl Read for FileReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buf),
            Self::FileLike(file) => file.read(buf),
        }
    }
}

impl FileReader {
    pub fn from_pyobject(path_or_file_like: PyObject) -> PyResult<Self> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        // is a path
        if let Ok(string_ref) = path_or_file_like.cast_as::<PyString>(py) {
            let path = string_ref.to_string_lossy().to_string();
            return Ok(Self::File(BufReader::new(File::open(path)?)));
        }

        // is a file-like
        match PyFileLikeObject::with_requirements(path_or_file_like, true, false, true) {
            Ok(f) => Ok(Self::FileLike(f)),
            Err(e) => Err(e),
        }
    }
}

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileWriter {
    File(BufWriter<File>),
    FileLike(PyFileLikeObject),
}

impl Seek for FileWriter {
    #[inline]
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(file) => file.seek(pos),
            Self::FileLike(file) => file.seek(pos),
        }
    }
}

impl Write for FileWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.write(buf),
            Self::FileLike(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(file) => file.flush(),
            Self::FileLike(file) => file.flush(),
        }
    }
}

impl FileWriter {
    pub fn from_pyobject(path_or_file_like: PyObject) -> PyResult<Self> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        // is a path
        if let Ok(string_ref) = path_or_file_like.cast_as::<PyString>(py) {
            let path = string_ref.to_string_lossy().to_string();
            return Ok(Self::File(BufWriter::new(File::create(path)?)));
        }

        // is a file-like
        match PyFileLikeObject::with_requirements(path_or_file_like, false, true, true) {
            Ok(f) => Ok(Self::FileLike(f)),
            Err(e) => Err(e),
        }
    }
}
