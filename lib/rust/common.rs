use byteorder::{BigEndian, ReadBytesExt};
use pyo3::prelude::*;

/// Get an i16, given a length-2 slice of u8's.
pub fn bytes_to_i16(bytes: &[u8]) -> Result<i16, &'static str> {
    let count = bytes.clone().read_i16::<BigEndian>().unwrap();
    Ok(count)
}

/// Get an i32, given a length-4 slice of u8's.
pub fn bytes_to_i32(bytes: &[u8]) -> Result<i32, &'static str> {
    let count = bytes.clone().read_i32::<BigEndian>().unwrap();
    Ok(count)
}

/// Get an i64, given a length-8 slice of u8's.
pub fn bytes_to_i64(bytes: &[u8]) -> Result<i64, &'static str> {
    let count = bytes.clone().read_i64::<BigEndian>().unwrap();
    Ok(count)
}

/// Simpler wrapper around pyo3's to_object() method.
pub fn to_pyobject_wrap<T>(x: T) -> PyObject
where
    T: pyo3::conversion::ToPyObject,
{
    Python::with_gil(|py| {
        return x.to_object(py);
    })
}
