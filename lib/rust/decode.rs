use enum_as_inner::EnumAsInner;
use pyo3::exceptions::*;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::hint::unreachable_unchecked;
use std::str::from_utf8;

use crate::common::{
    bytes_to_bool, bytes_to_f32, bytes_to_f64, bytes_to_i16, bytes_to_i32, bytes_to_i64,
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
    fn to_object(&self, py: Python) -> PyObject {
        match &self {
            PgData::Varchar(_) => {
                return self.as_varchar().unwrap().to_object(py);
            }
            PgData::Smallint(_) => {
                return self.as_smallint().unwrap().to_object(py);
            }
            PgData::Integer(_) => {
                return self.as_integer().unwrap().to_object(py);
            }
            PgData::Bigint(_) => {
                return self.as_bigint().unwrap().to_object(py);
            }
            PgData::Real(_) => {
                return self.as_real().unwrap().to_object(py);
            }
            PgData::Double(_) => {
                return self.as_double().unwrap().to_object(py);
            }
            PgData::Boolean(_) => {
                return self.as_boolean().unwrap().to_object(py);
            }
        }
    }
}

#[pymethods]
impl _ParseDataTypes {
    #[new]
    pub fn new(raw: Vec<Option<Vec<u8>>>, data_type: String) -> PyResult<Self> {
        // Check for allowed data types
        let allowed = vec![
            "varchar", "integer", "smallint", "bigint", "real", "double", "boolean",
        ];
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
        let mut out = vec![None; self.raw.len()];
        out.par_iter_mut().enumerate().for_each(|(i, parsed)| {
            let e = self.raw[i].as_ref();
            match e {
                None => {}
                Some(e) => match self.data_type.as_ref() {
                    "varchar" => {
                        let decoded = from_utf8(e).unwrap();
                        let enummed = PgData::Varchar(String::from(decoded));
                        *parsed = Some(enummed)
                    }
                    "integer" => *parsed = Some(parse_pg_bytes![e, PgData::Integer, bytes_to_i32]),
                    "smallint" => {
                        *parsed = Some(parse_pg_bytes![e, PgData::Smallint, bytes_to_i16])
                    }
                    "bigint" => *parsed = Some(parse_pg_bytes![e, PgData::Bigint, bytes_to_i64]),
                    "real" => *parsed = Some(parse_pg_bytes![e, PgData::Real, bytes_to_f32]),
                    "double" => *parsed = Some(parse_pg_bytes![e, PgData::Double, bytes_to_f64]),
                    "boolean" => *parsed = Some(parse_pg_bytes![e, PgData::Boolean, bytes_to_bool]),
                    _ => unsafe {
                        // This should not be reached due to the check
                        // in the new function.
                        unreachable_unchecked()
                    },
                },
            }
        });
        self.decoded = Some(out);
        Ok(())
    }

    #[getter]
    fn decoded(&self) -> PyResult<Vec<Option<PyObject>>> {
        let mut out = Vec::new();

        Python::with_gil(|py| {
            for d in self.decoded.as_ref().unwrap() {
                match d {
                    None => out.push(None),
                    Some(x) => out.push(Some(x.to_object(py))),
                }
            }
        });
        Ok(out)
    }
}
