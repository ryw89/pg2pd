use bitvec_rs::BitVec;
use byteorder::{BigEndian, ReadBytesExt};
use log::debug;
use pyo3::exceptions::*;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::fs::read;
use std::io::ErrorKind;

/// Count number of fields in a data tuple
fn get_tuple_fields_count(bytes: &[u8]) -> Result<i16, &'static str> {
    if bytes.len() != 2 {
        return Err("Expected a 2-length vector of u8; cannot parse other lengths.");
    }
    let count = bytes.as_ref().read_i16::<BigEndian>().unwrap();
    Ok(count)
}

/// Find beginning of data in Postgres copy binary data. Intended to
/// be used as a vector index of _ParsePgBinary.bytes.
fn find_data_beginning(option_fixed_header_size: Option<u32>, header_ext_len: u32) -> usize {
    // The data on header extension length is a 32-bit integer, which is
    // the 4 bytes here.
    let header_ext_len_incl_len_data = 4 + header_ext_len;

    // There's no plan to change the fixed size of the data header
    // from 15 bytes, but this is here in case that changes.
    match option_fixed_header_size {
        Some(s) => (s + header_ext_len_incl_len_data) as usize,
        None => (15 + header_ext_len_incl_len_data) as usize,
    }
}

/// Get the length of a field in bytes, given a length-4 slice of
/// u8's.
fn get_data_length(bytes: &[u8]) -> Result<i32, &'static str> {
    if bytes.len() != 4 {
        return Err("Expected a 4-length vector of u8; cannot parse other lengths.");
    }
    let count = bytes.as_ref().read_i32::<BigEndian>().unwrap();
    Ok(count)
}

fn get_field_min_max(fields_per_row: &[i16]) -> Result<(i16, i16), &'static str> {
    Ok((
        *fields_per_row.iter().min().unwrap(),
        *fields_per_row.iter().max().unwrap(),
    ))
}

/// Location and length in bytes of a Postgres binary data field.
#[derive(Clone, Copy)]
struct FieldLocation {
    start: usize,
    byte_len: i32,
}

#[pyclass]
pub struct _ParsePgBinary {
    path: String,
    bytes: Option<Vec<u8>>,
    has_oids: Option<bool>,
    header_ext_len: Option<u32>,
    field_locations: Option<Vec<FieldLocation>>,
    fields_per_row: Option<Vec<i16>>,
}

/// Parse a Postgres binary file.
///
/// Code is based on the documentation available at
/// https://www.postgresql.org/docs/14.0/sql-copy.html.
#[pymethods]
impl _ParsePgBinary {
    #[new]
    pub fn new(path: String) -> Self {
        _ParsePgBinary {
            path,
            bytes: None,
            has_oids: None,
            header_ext_len: None,
            field_locations: None,
            fields_per_row: None,
        }
    }

    /// Read data into a vector of bytes.
    pub fn read_file(&mut self) -> PyResult<()> {
        match read(&self.path) {
            Ok(bytes) => self.bytes = Some(bytes),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    return Err(PyFileNotFoundError::new_err(format!(
                        "File '{}' not found.",
                        &self.path
                    )))
                }
                _ => panic!("{}", e),
            },
        }
        Ok(())
    }

    /// Validate file header. A valid Postgres binary copy should
    /// contain the fifteen bytes "PGCOPY\n\xff\r\n\x00" at the
    /// beginning.
    pub fn validate_header(&self) -> PyResult<()> {
        // First, let's confirm that self.bytes is a Some(). Exception
        // if not.
        if self.bytes.is_none() {
            return Err(PyValueError::new_err("File has not been loaded yet."));
        }

        let magic_number = b"PGCOPY\n\xff\r\n\x00";
        let first_eleven_bytes = &self.bytes.as_ref().unwrap()[..11];

        for (a, b) in magic_number.iter().zip(first_eleven_bytes.iter()) {
            if a != b {
                return Err(PyValueError::new_err(
                    "File does not appear to be a valid Postgres binary copy file.",
                ));
            }
        }
        Ok(())
    }

    /// Check if data has OIDs. The fifteenth bit of bytes 12 to 15
    /// will contain the OID flag.
    pub fn check_oids(&mut self) -> PyResult<()> {
        let bytes_twelve_to_fifteen = &self.bytes.as_ref().unwrap()[11..15];
        let bits = BitVec::from_bytes(bytes_twelve_to_fifteen);

        // Bit 16 of bytes twelve to fifteen is the OID flag
        self.has_oids = Some(bits[15]);
        Ok(())
    }

    /// Find header extension length. With current versions of
    /// Postgres, this will always be zero, but this could change in
    /// the future.
    pub fn get_header_ext_len(&mut self) -> PyResult<()> {
        // Length is 32-bit integer at byte 16
        let bytes = &self.bytes.as_ref().unwrap()[15..19];
        self.header_ext_len = Some(bytes.as_ref().read_u32::<BigEndian>().unwrap());
        debug!(
            "Header extension length is {}",
            self.header_ext_len.unwrap()
        );
        Ok(())
    }

    /// Find and index locations of all data fields and their length.
    pub fn find_data_locations(&mut self) -> PyResult<()> {
        // First address should be at "number of fields in current
        // tuple" 16-bit integer
        let mut current_address = find_data_beginning(None, self.header_ext_len.unwrap());

        // This initial value of zero will force a check on the first
        // loop iteration
        let mut num_fields_left_for_current_tuple = 0;

        let mut field_locations = Vec::new();
        let mut fields_per_row = Vec::new();
        let bytes = &self.bytes.as_ref().unwrap();
        loop {
            debug!("Current index is: {}", current_address);
            debug!(
                "Number of fields left for this tuple: {}",
                num_fields_left_for_current_tuple
            );

            // First, let's check if we're at an address with "number
            // of fields" data or "field length" data.
            if num_fields_left_for_current_tuple == 0 {
                // In this case, we should be at a "number of fields"
                // 16-bit integer.
                let current_slice = &bytes[current_address..current_address + 2];
                num_fields_left_for_current_tuple = get_tuple_fields_count(current_slice).unwrap();

                // Must check if hit data footer -- Will contain -1
                if num_fields_left_for_current_tuple == -1 {
                    debug!("Found data footer.");
                    break;
                }

                // Add number of fields in this row to vector
                fields_per_row.push(num_fields_left_for_current_tuple);

                // Move the address by 16 bits to the next bit of "field length" data
                current_address += 2;
            } else {
                // We should be at a 32-bit "field length" piece of
                // data, so let's get the data length first.
                let current_slice = &bytes[current_address..current_address + 4];
                let data_len = get_data_length(current_slice).unwrap();

                // Let's move past the 32-bit "field length" integer to the data itself
                current_address += 4;

                // And now we can push the location of the field and
                // its length in bytes.
                let this_field_location = FieldLocation {
                    start: current_address,
                    byte_len: data_len,
                };
                field_locations.push(this_field_location);

                if data_len != -1 {
                    // A -1 length is a special case indicating NULL
                    // Postgres data; the next "field length" bit of
                    // data follows immediately. But otherwise, we'll
                    // move length of the actual data.
                    current_address += data_len as usize;
                }

                // Decrement counter of number of fields left in this tuple.
                num_fields_left_for_current_tuple += -1;
            }
        }
        self.field_locations = Some(field_locations);
        self.fields_per_row = Some(fields_per_row);
        Ok(())
    }

    pub fn get_num_fields(&self) -> PyResult<usize> {
        let tup = get_field_min_max(self.fields_per_row.as_ref().unwrap());
        let (min_fields, max_fields) = tup.unwrap();
        if min_fields != max_fields {
            // Currently, only binaries with the same number of fields in
            // each row are supported. This is all that Postgres itself
            // should create, but this could change in the future.
            return Err(PyNotImplementedError::new_err(
                "Postgres binary copy files with different numbers of fields per row are not supported."));
        }

        Ok(min_fields as usize)
    }

    /// Fetch a vector of byte vectors for a specific column.
    pub fn get_col_bytes(&self, col: usize) -> PyResult<Vec<Option<Vec<u8>>>> {
        // Check for number of fields
        let num_fields = self.get_num_fields().unwrap();

        // Check for valid column index
        if col > num_fields - 1 {
            return Err(PyIndexError::new_err(format!(
                "Data has only {} column(s)",
                num_fields
            )));
        }

        // For later iteration through field locations vector,
        // stepping by number of fields in each column
        let end = self.field_locations.as_ref().unwrap().len();
        let ranges: Vec<usize> = (col..end).step_by(num_fields).collect();

        // Output vector
        let mut out: Vec<Option<Vec<u8>>> = vec![None; ranges.len()];

        out.par_iter_mut().enumerate().for_each(|(i, field_bytes)| {
            let field_data_idx = ranges[i];
            let field_data = self.field_locations.as_ref().unwrap()[field_data_idx];
            let start = field_data.start;
            let byte_len = field_data.byte_len;

            if byte_len != -1 {
                // The typical case -- Clone the byte vector slice and
                // insert it. Otherwise, we'll leave the None value as it is
                // to represent a Postgres NULL.
                let dst = self.bytes.as_ref().unwrap()[start..start + byte_len as usize].to_vec();
                *field_bytes = Some(dst);
            }
        });
        Ok(out)
    }

    pub fn get_col_bytes_all(&self) -> PyResult<Vec<Vec<Option<Vec<u8>>>>> {
        let num_fields = self.get_num_fields().unwrap();
        let empty_vec = vec![None];
        let mut out = vec![empty_vec; num_fields];
        out.par_iter_mut().enumerate().for_each(|(i, col)| {
            *col = self.get_col_bytes(i).unwrap();
        });
        Ok(out)
    }

    #[getter]
    fn path(&self) -> PyResult<String> {
        Ok(self.path.clone())
    }

    #[getter]
    fn bytes(&self) -> PyResult<Option<Vec<u8>>> {
        match &self.bytes {
            Some(b) => Ok(Some(b.clone())),
            None => Err(PyAttributeError::new_err(
                "'_ParsePgBinary' object has no attribute 'bytes'",
            )),
        }
    }

    #[getter]
    fn field_locations(&self) -> PyResult<Vec<(usize, i32)>> {
        let mut out = Vec::new();
        for f in self.field_locations.as_ref().unwrap().iter() {
            let this_start = f.start;
            let this_byte_len = f.byte_len;
            out.push((this_start, this_byte_len));
        }
        Ok(out)
    }

    #[getter]
    fn fields_per_row(&self) -> PyResult<Option<Vec<i16>>> {
        match &self.fields_per_row {
            Some(f) => Ok(Some(f.clone())),
            None => Err(PyAttributeError::new_err(
                "'_ParsePgBinary' object has no attribute 'fields_per_row'",
            )),
        }
    }

    #[getter]
    fn num_fields(&self) -> PyResult<usize> {
        self.get_num_fields()
    }
}
