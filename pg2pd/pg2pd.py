from typing import Optional

import pandas as pd

from .lib.rust import _ParseDataTypes, _ParsePgBinary


def pg_binary_to_bytes(path: str,
                       expected_col_num: Optional[int] = None) -> list[int]:
    """Fetch all Postgres columns as vectors of bytes."""
    pg = _ParsePgBinary(path)
    pg.read_file()
    pg.validate_header()
    pg.get_header_ext_len()
    pg.check_oids()
    pg.find_data_locations()

    if expected_col_num is not None:
        if pg.num_fields != expected_col_num:
            raise ValueError(
                f'Expected {expected_col_num} column(s), but found {pg.num_fields} from parsing.'
            )

    return pg.get_col_bytes_all()


class Pg2Pd():
    def __init__(self,
                 path: str,
                 schema: list[str],
                 colnames: Optional[list[str]] = None):
        self.path = path

        self.schema = schema
        self._validate_schema()

        if colnames:
            self.colnames = colnames
            self._validate_colnames()
        else:
            self.colnames = [str(x) for x in range(len(self.schema))]

    def _validate_schema(self):
        # First, check that list is all strings.
        msg = 'Schema must be a list of strings.'
        if not isinstance(self.schema, list):
            raise ValueError(msg)

        if not all([isinstance(x, str) for x in self.schema]):
            raise ValueError(msg)

        # Now validate the schema and make a schema free of aliases
        unaliased = []
        found = [False] * len(self.schema)
        for i, s in enumerate(self.schema):
            if s in self._type_aliases.keys():
                unaliased.append(s)
                found[i] = True
                continue

            for k, v in self._type_aliases.items():
                if s in v:
                    unaliased.append(k)
                    found[i] = True
                    continue

        if not all(found):
            bad_types = list(
                set([x for x, y in zip(self.schema, found) if not y]))
            if len(bad_types) <= 3:
                msg_base = 'Invalid types found: '
            else:
                msg_base = 'Invalid types found, including: '

            msg = msg_base + ', '.join(bad_types[:3]) + '.'
            raise ValueError(msg)

        self._unaliased_schema = unaliased

    def _validate_colnames(self):
        if len(self.colnames) != len(self.schema):
            raise ValueError(
                '\'colnames\' and \'schema\' must be same length.')

        msg = 'Column names must be a list of strings.'
        if not isinstance(self.schema, list):
            raise ValueError(msg)

        if not all([isinstance(x, str) for x in self.schema]):
            raise ValueError(msg)

    @property
    def _type_aliases(self):
        return {
            'varchar': ['str', 'string', 'text'],
            'integer': ['i32'],
            'smallint': ['i16'],
            'bigint': ['i64'],
            'real': ['f32'],
            'double': ['f64'],
            'boolean': ['bool']
        }

    def _parse_pg_binary(self):
        """Main invocation of Postgres binary copy parser."""
        self._cols_as_bytes = pg_binary_to_bytes(self.path, len(self.schema))

    def make_df(self, return_df: bool = True):
        """Make a Pandas dataframe from Postgres binary copy data."""
        if not hasattr(self, '_col_as_bytes'):
            self._parse_pg_binary()

        self._cols = {}
        for colname, col, dtype in zip(self.colnames, self._cols_as_bytes,
                                       self._unaliased_schema):
            parse = _ParseDataTypes(col, dtype)
            parse.parse_data()
            self._cols[colname] = parse.decoded

        df = pd.DataFrame(self._cols, columns=self.colnames)
        self.df = df
        if return_df:
            return self.df
