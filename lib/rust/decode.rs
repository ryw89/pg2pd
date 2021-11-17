use enum_as_inner::EnumAsInner;
use pyo3::exceptions::*;
use pyo3::prelude::*;
use std::str::from_utf8;

use crate::common::{bytes_to_i16, bytes_to_i32, bytes_to_i64, to_pyobject_wrap};

#[pyclass]
pub struct _ParseDataTypes {
    raw: Vec<Option<Vec<u8>>>,
    data_type: String,
    decoded: Option<Vec<Option<PgData>>>,
}

#[derive(Clone, EnumAsInner)]
enum PgData {
    Varchar(String),
    Smallint(i16),
    Integer(i32),
    Bigint(i64),
    Real(f32),
    Double(f64),
}

impl ToPyObject for PgData {
    fn to_object(&self, _py: Python) -> PyObject {
        match &self {
            PgData::Varchar(_) => {
                return to_pyobject_wrap(&self.as_varchar().unwrap());
            }
            PgData::Smallint(_) => {
                return to_pyobject_wrap(&self.as_smallint().unwrap());
            }
            PgData::Integer(_) => {
                return to_pyobject_wrap(&self.as_integer().unwrap());
            }
            PgData::Bigint(_) => {
                return to_pyobject_wrap(&self.as_bigint().unwrap());
            }
            PgData::Real(_) => {
                return to_pyobject_wrap(&self.as_real().unwrap());
            }
            PgData::Double(_) => {
                return to_pyobject_wrap(&self.as_double().unwrap());
            }
        }
    }
}

#[pymethods]
impl _ParseDataTypes {
    #[new]
    pub fn new(raw: Vec<Option<Vec<u8>>>, data_type: String) -> PyResult<Self> {
        // Check for allowed data types
        let allowed = vec!["varchar", "integer", "smallint", "bigint"];
        if !allowed.iter().any(|&i| i == data_type) {
            return Err(PyValueError::new_err(format!(
                "Invalid data type: {}.",
                data_type
            )));
        }

        Ok(_ParseDataTypes {
            raw,
            data_type,
            decoded: None,
        })
    }

    pub fn parse_data(&mut self) -> PyResult<()> {
        let mut out = Vec::new();
        for e in &self.raw {
            match e {
                None => out.push(None),
                Some(e) => {
                    if &self.data_type == "varchar" {
                        let decoded = from_utf8(e).unwrap();
                        let enummed = PgData::Varchar(String::from(decoded));
                        out.push(Some(enummed));
                    } else if &self.data_type == "integer" {
                        let decoded = bytes_to_i32(e).unwrap();
                        let enummed = PgData::Integer(decoded);
                        out.push(Some(enummed));
                    } else if &self.data_type == "smallint" {
                        let decoded = bytes_to_i16(e).unwrap();
                        let enummed = PgData::Smallint(decoded);
                        out.push(Some(enummed));
                    } else if &self.data_type == "bigint" {
                        let decoded = bytes_to_i64(e).unwrap();
                        let enummed = PgData::Bigint(decoded);
                        out.push(Some(enummed));
                    }
                }
            }
        }
        self.decoded = Some(out);
        Ok(())
    }

    #[getter]
    fn decoded(&self) -> PyResult<Vec<Option<PyObject>>> {
        let mut out = Vec::new();

        for d in self.decoded.as_ref().unwrap() {
            match d {
                None => out.push(None),
                Some(x) => {
                    Python::with_gil(|py| out.push(Some(x.to_object(py))));
                }
            }
        }
        Ok(out)
    }
}
