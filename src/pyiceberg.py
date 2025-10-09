import io
import sys
from typing import Any
from pyiceberg.catalog import load_catalog, Catalog
import sqlalchemy.exc
import pyarrow as pa
from pyarrow import csv

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


def upload_to_iceberg(df: pa.Table) -> None:
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
            tbl = catalog.load_table(table)
        else:
            print('Table does not exist')
            sys.exit(1)
    except sqlalchemy.exc.OperationalError as e:
        print('Error connecting to the database:', e)
        sys.exit(1)
    except OSError as e:
        print('Error accessing S3 storage:', e)
        sys.exit(1)

    # Ensure the dataframe matches the table schema
    schema: pa.Schema = tbl.schema().as_arrow()
    df = df.cast(schema)

    tbl.upsert(df, ['id'])


def write_to_table(*args: Any, **kwargs: Any) -> None:
    source = io.BytesIO(bytes(args[0]))
    table: pa.Table = csv.read_csv(source, read_options=csv.ReadOptions(
        column_names=['id', 'name', 'salary'], encoding='utf8')
    )
    upload_to_iceberg(table)
