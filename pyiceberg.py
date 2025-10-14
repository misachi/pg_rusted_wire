import io
import sys
from typing import Any, List, Optional
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table, upsert_util
import sqlalchemy.exc
import pyarrow as pa
from pyarrow import csv
import pyarrow.compute as pc

# S3 access details
S3_SECRET_KEY = 'pass1234'
S3_ENDPOINT = 'http://1.1.1.1:9000'
S3_ACCESS_KEY = 'minio_user'

# Catalog connection details
# Postgres Catalog URI
CATALOG_URI = 'postgresql+psycopg2://<username>:<password>@2.2.2.2:5432/iceberg'

# Iceberg table details

# Full table name in the format catalog.schema.table
TABLE_NAME = 'catalog_todo.example_s3_schema.employees_test'
ICEBERG_CATALOG, ICEBERG_SCHEMA, ICEBERG_TABLE = TABLE_NAME.split('.')


def parse_config_file(file_path: str) -> None:
    global S3_SECRET_KEY, S3_ENDPOINT, S3_ACCESS_KEY, CATALOG_URI, TABLE_NAME
    try:
        with open(file_path, 'r') as f:
            for line in f:
                key, value = line.strip().split('=', 1)
                if key == 'S3_SECRET_KEY':
                    S3_SECRET_KEY = value.strip("'\"")
                elif key == 'S3_ENDPOINT':
                    S3_ENDPOINT = value.strip("'\"")
                elif key == 'S3_ACCESS_KEY':
                    S3_ACCESS_KEY = value.strip("'\"")
                elif key == 'CATALOG_URI':
                    CATALOG_URI = value.strip("'\"")
                elif key == 'TABLE_NAME':
                    TABLE_NAME = value.strip("'\"")
                    global ICEBERG_CATALOG, ICEBERG_SCHEMA, ICEBERG_TABLE
                    ICEBERG_CATALOG, ICEBERG_SCHEMA, ICEBERG_TABLE = TABLE_NAME.split(
                        '.')
    except FileNotFoundError:
        print('Configuration file not found. Using default settings.')
    except Exception as e:
        print(f'Error reading configuration file: {e}')
        sys.exit(1)


def get_table(file_path: str) -> Table:
    parse_config_file(file_path)

    catalog: Catalog = load_catalog(
        ICEBERG_CATALOG,
        **{
            'uri': CATALOG_URI,
            's3.endpoint': S3_ENDPOINT,
            'py-io-impl': 'pyiceberg.io.pyarrow.PyArrowFileIO',
            's3.access-key-id': S3_ACCESS_KEY,
            's3.secret-access-key': S3_SECRET_KEY,
            'init_catalog_tables': 'false'
        }
    )

    try:
        table = '{}.{}'.format(ICEBERG_SCHEMA, ICEBERG_TABLE)
        if catalog.table_exists(table):
            return catalog.load_table(table)
        else:
            print('Table does not exist')
            sys.exit(1)
    except sqlalchemy.exc.OperationalError as e:
        print('Error connecting to the database:', e)
        sys.exit(1)
    except OSError as e:
        print('Error accessing S3 storage:', e)
        sys.exit(1)


def upload_to_iceberg(df: pa.Table, *args: Any) -> None:
    tbl: Table = get_table(args[1])

    # Ensure the dataframe matches the table schema. If key columns are
    # provided, use them for upsert. If not, use all columns. Also when no
    # columns are provided, use the iceberg table schema. This is applicable
    # when performing the initial snapshot loading.
    schema: Optional[pa.Schema] = None
    identifier_fields: List[Optional[str]] = []
    if args[3]:
        schema = tbl.schema()
        identifier_fields = args[3]
    else:
        source = io.BytesIO(bytes(args[0]))
        df = csv.read_csv(source, read_options=csv.ReadOptions(
            column_names=tbl.metadata.schema().column_names, encoding='utf8')
        )

        schema = tbl.schema()
        # Use all columns from the iceberg table schema
        identifier_fields = tbl.metadata.schema().column_names

    # Remove rows with required fields, that are nulls, from table
    required_fields = [
        field.name for field in schema.columns if field.optional is not True]

    print(f'The {required_fields} fields are always required. If any one of the fields is null, the row will be dropped')
    filter_mask = None
    for field in required_fields:
        if filter_mask is None:
            filter_mask = pc.is_valid(df[field])
        else:
            filter_mask = pc.or_(filter_mask, pc.is_valid(df[field]))

    df = df.filter(filter_mask)
    df = df.cast(schema.as_arrow())
    tbl.upsert(df, identifier_fields)


def write_to_table(*args: Any, **kwargs: Any) -> None:
    if len(args) != 4:
        raise ValueError(
            'Expected exactly 4 arguments: data, config_file_path, columns and key_columns')

    source = io.BytesIO(bytes(args[0]))
    table: Optional[pa.Table] = None

    if len(args[0]) <= 0:
        raise ValueError('No data to write')

    if args[2]:
        table = csv.read_csv(source, read_options=csv.ReadOptions(
            column_names=args[2], encoding='utf8')
        )

    upload_to_iceberg(table, *args)


def delete_from_table(*args: Any, **kwargs: Any) -> None:
    if len(args) != 4:
        raise ValueError(
            'Expected exactly 4 arguments: data, config_file_path, columns and key_columns')

    source = io.BytesIO(bytes(args[0]))
    df: Optional[pa.Table] = None
    tbl: Optional[Table] = None

    if args[2]:
        df = csv.read_csv(source, read_options=csv.ReadOptions(
            column_names=args[2], encoding='utf8')
        )
    else:
        tbl = get_table(args[1])
        df = csv.read_csv(source, read_options=csv.ReadOptions(
            column_names=tbl.metadata.schema().column_names, encoding='utf8')
        )

    if not df:
        raise ValueError('Nothing to delete')

    if not tbl:
        tbl = get_table(args[1])

    matched_predicate = upsert_util.create_match_filter(df, args[3])
    tbl.delete(delete_filter=matched_predicate)
