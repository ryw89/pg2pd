import tempfile

from pg2pd import Pg2Pd


def test_make_df_1(pg_conn):
    """Test of main Postgres binary data to Pandas dataframe pipeline.

    This tests an integer and varchar.
    """
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


def test_make_df_2(capsys, pg_conn):
    """Test of main Postgres binary data to Pandas dataframe pipeline.

    This tests boolean data.
    """
    cursor = pg_conn.cursor()

    # Copy binary data to a tempfile
    path = tempfile.mkstemp()[1]
    query = 'COPY test2 TO STDOUT BINARY;'
    with open(path, 'wb') as f:
        cursor.copy_expert(sql=query, file=f)
        pg_conn.commit()

        pg = Pg2Pd(path, ['boolean', 'boolean'], ['t', 'f'])
    df = pg.make_df()

    assert df['t'].tolist() == [True]
    assert df['f'].tolist() == [False]
