import tempfile

import pandas as pd

from pg2pd import Pg2Pd


def test_perf_pg2pd_1(benchmark, pg_conn):
    """Basic benchmark of pg.make_df function."""
    cursor = pg_conn.cursor()

    # Copy binary data to a tempfile
    path = tempfile.mkstemp()[1]
    query = 'COPY (SELECT generate_series(1,1000) AS id, substr(md5(random()::text), 0, 25) AS data) TO STDOUT BINARY;'
    with open(path, 'wb') as f:
        cursor.copy_expert(sql=query, file=f)
        pg_conn.commit()

        pg = Pg2Pd(path, ['integer', 'text'], ['id', 'data'])

    benchmark(pg.make_df)

    assert True


def test_perf_pd_1(benchmark, pg_conn):
    """Test of Pandas' read_csv, for comparison."""
    cursor = pg_conn.cursor()

    # Copy binary data to a tempfile
    path = tempfile.mkstemp()[1]
    query = 'COPY (SELECT generate_series(1,1000) AS id, substr(md5(random()::text), 0, 25) AS data) TO STDOUT CSV;'
    with open(path, 'wb') as f:
        cursor.copy_expert(sql=query, file=f)
        pg_conn.commit()

    benchmark(pd.read_csv, path)

    assert True
