import psycopg2
import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer


@pytest.fixture
def pg_conn(postgres_container_creds):
    conn = psycopg2.connect(host=postgres_container_creds['host'],
                            port=postgres_container_creds['port'],
                            user=postgres_container_creds['user'],
                            password=postgres_container_creds['password'],
                            database=postgres_container_creds['dbname'])

    yield conn


@pytest.fixture(scope='module')
def postgres_container_creds():
    """Initialize a Postgres container with data, and return its
    credentials.
    """
    with PostgresContainer('postgres:14.0') as postgres:

        # Create tables and insert data.
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        meta = sqlalchemy.MetaData(engine)

        sqlalchemy.Table('test1', meta,
                         sqlalchemy.Column('id', sqlalchemy.Integer),
                         sqlalchemy.Column('text', sqlalchemy.String))

        meta.create_all(engine)

        # Insert test data
        engine.execute("""INSERT INTO test1(id, text)
                          VALUES (42, 'Some cool data')""")
        engine.execute("""INSERT INTO test1(id, text)
                          VALUES (25, 'Even more cool data')""")

        # Return Docker container's postgres credentials
        port = postgres.get_exposed_port(5432)
        creds = {
            'dbname': 'test',
            'host': 'localhost',
            'port': port,
            'user': 'test',
            'password': 'test'
        }

        yield creds