# `pg2pd` -- A Postgres to Pandas parser written in Python and Rust

This package parses Postgres [binary copy
data](https://www.postgresql.org/docs/14/sql-copy.html) and places it
into Pandas dataframes.

# Requirements

The parser itself is written as a Rust extension module using
[PyO3](https://github.com/PyO3/pyo3), so you'll need an installation
of Rust.

# Supported [Postgres data types](https://www.postgresql.org/docs/14/datatype.html)

- [x] text
- [x] integer
- [ ] smallint
- [ ] bigint
- [ ] boolean
- [ ] real
- [ ] double precision
