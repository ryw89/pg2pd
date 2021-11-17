import tempfile

from pg2pd import Pg2Pd


def test_make_df(pg_conn):
    """Test of main Postgres binary data to Pandas dataframe pipeline."""
    cursor = pg_conn.cursor()

    # Copy binary data to a tempfile
    path = tempfile.mkstemp()[1]
    query = 'COPY test1 TO STDOUT BINARY;'
    with open(path, 'wb') as f:
        cursor.copy_expert(sql=query, file=f)
        pg_conn.commit()

    pg = Pg2Pd(path, ['integer', 'varchar'], ['id', 'text'])
    df = pg.make_df()

    assert df['id'].tolist() == [42, 25]
    assert df['text'].tolist() == ['Some cool data', 'Even more cool data']
