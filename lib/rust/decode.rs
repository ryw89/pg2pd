use enum_as_inner::EnumAsInner;
use pyo3::exceptions::*;
use pyo3::prelude::*;
use std::str::from_utf8;

use crate::common::{
    bytes_to_bool, bytes_to_f32, bytes_to_f64, bytes_to_i16, bytes_to_i32, bytes_to_i64,
    to_pyobject_wrap,
};

macro_rules! parse_pg_bytes {
    ( $x:expr, $pg_data_type:expr, $decode_fun:expr ) => {{
        let decoded = $decode_fun($x).unwrap();
        let enummed = $pg_data_type(decoded);
        enummed
    }};
}

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
    Boolean(bool),
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
            PgData::Boolean(_) => {
                return to_pyobject_wrap(&self.as_boolean().unwrap());
            }
        }
    }
}

#[pymethods]
impl _ParseDataTypes {
    #[new]
    pub fn new(raw: Vec<Option<Vec<u8>>>, data_type: String) -> Self {
        _ParseDataTypes {
            raw,
            data_type,
            decoded: None,
        }
    }

    pub fn parse_data(&mut self) -> PyResult<()> {
        let mut out = Vec::new();
        for e in &self.raw {
            match e {
                None => out.push(None),
                Some(e) => match self.data_type.as_ref() {
                    "varchar" => {
                        let decoded = from_utf8(e)?;
                        let enummed = PgData::Varchar(String::from(decoded));
                        out.push(Some(enummed));
                    }
                    "integer" => out.push(Some(parse_pg_bytes![e, PgData::Integer, bytes_to_i32])),
                    "smallint" => {
                        out.push(Some(parse_pg_bytes![e, PgData::Smallint, bytes_to_i16]))
                    }
                    "bigint" => out.push(Some(parse_pg_bytes![e, PgData::Bigint, bytes_to_i64])),
                    "real" => out.push(Some(parse_pg_bytes![e, PgData::Real, bytes_to_f32])),
                    "double" => out.push(Some(parse_pg_bytes![e, PgData::Double, bytes_to_f64])),
                    "boolean" => out.push(Some(parse_pg_bytes![e, PgData::Boolean, bytes_to_bool])),
                    _ => {
                        return Err(PyValueError::new_err(format!(
                            "Invalid data type: {}.",
                            self.data_type
                        )))
                    }
                },
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
