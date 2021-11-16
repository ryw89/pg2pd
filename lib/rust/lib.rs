use pyo3::prelude::*;

mod binary;
mod common;
mod decode;

#[pymodule]
fn __pg2pd_rust(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<binary::_ParsePgBinary>()?;
    m.add_class::<decode::_ParseDataTypes>()?;

    Ok(())
}
