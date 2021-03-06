use byteorder::{BigEndian, ReadBytesExt};
use pyo3::exceptions::*;
use pyo3::prelude::*;

/// Get an i16, given a length-2 slice of u8's.
pub fn bytes_to_i16(bytes: &[u8]) -> Result<i16, &'static str> {
    let count = bytes.as_ref().read_i16::<BigEndian>().unwrap();
    Ok(count)
}

/// Get an i32, given a length-4 slice of u8's.
pub fn bytes_to_i32(bytes: &[u8]) -> Result<i32, &'static str> {
    let count = bytes.as_ref().read_i32::<BigEndian>().unwrap();
    Ok(count)
}

/// Get an i64, given a length-8 slice of u8's.
pub fn bytes_to_i64(bytes: &[u8]) -> Result<i64, &'static str> {
    let count = bytes.as_ref().read_i64::<BigEndian>().unwrap();
    Ok(count)
}

/// Get an f32, given a length-4 slice of u8's.
pub fn bytes_to_f32(bytes: &[u8]) -> Result<f32, &'static str> {
    let count = bytes.as_ref().read_f32::<BigEndian>().unwrap();
    Ok(count)
}

/// Get an f64, given a length-4 slice of u8's.
pub fn bytes_to_f64(bytes: &[u8]) -> Result<f64, &'static str> {
    let count = bytes.as_ref().read_f64::<BigEndian>().unwrap();
    Ok(count)
}

/// Get a bool, given a length-1 slice of u8's.
pub fn bytes_to_bool(bytes: &[u8]) -> PyResult<bool> {
    if bytes.len() != 1 {
        return Err(PyValueError::new_err(format!(
            "Invalid length of u8 slice: expected 1, got {}.",
            bytes.len()
        )));
    }
    if bytes[0] == 1 {
        Ok(true)
    } else {
        Ok(false)
    }
}
