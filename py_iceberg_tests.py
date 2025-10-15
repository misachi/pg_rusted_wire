# Run with: pytest py_iceberg_tests.py

import tempfile
import os

from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType

from py_iceberg import delete_from_table, write_to_table

NAMESPACE = 'test_ns'
TABLE_NAME = f'{NAMESPACE}.test_employee'
WAREHOUSE_PATH = 'warehouse'
DEFAULT_CONFIG_DATA = f"""S3_SECRET_KEY='test'
            S3_ENDPOINT='test'
            S3_ACCESS_KEY='test'
            CATALOG_URI='test'
            TABLE_NAME='test.{TABLE_NAME}'"""


def create_table(namespace: str, table_name: str, catalog: Catalog):
    schema = Schema(
        NestedField(field_id=1, name="id",
                    field_type=LongType(), required=True),
        NestedField(field_id=2, name="name",
                    field_type=StringType(), required=False),
        NestedField(field_id=3, name="salary",
                    field_type=DoubleType(), required=False)
    )
    catalog.create_namespace(namespace)
    catalog.create_table(
        identifier=table_name,
        schema=schema,
        properties={"format-version": "2"}
    )


def get_catalog(warehouse_path):
    return load_catalog(
        "default",
        **{
            'type': 'sql',
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )


def test_insert():
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse_path = os.path.join(tmpdir, WAREHOUSE_PATH)
        os.makedirs(warehouse_path)

        catalog = get_catalog(warehouse_path)

        create_table(NAMESPACE, TABLE_NAME, catalog)

        config_path = os.path.join(tmpdir, 'config')

        with open(config_path, 'w') as f:
            f.write(DEFAULT_CONFIG_DATA)

        # Insert 3 employees
        data = b'1,Simba Kwach,25000\n2,Masiza Omota,2309\n3,Da Capo,3000'
        write_to_table(
            data, config_path, ['id', 'name', 'salary'],
            ['id'], catalog=catalog)

        tbl = catalog.load_table(TABLE_NAME)
        arrow_table = tbl.scan().to_arrow()

        assert arrow_table.num_rows == 3


def test_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse_path = os.path.join(tmpdir, WAREHOUSE_PATH)
        os.makedirs(warehouse_path)

        catalog = get_catalog(warehouse_path)

        create_table(NAMESPACE, TABLE_NAME, catalog)

        config_path = os.path.join(tmpdir, 'config')

        with open(config_path, 'w') as f:
            f.write(DEFAULT_CONFIG_DATA)

        insert_data = \
            b'1,Simba Kwach,25000\n2,Masiza Omota,2309\n3,Da Capo,3000'
        write_to_table(insert_data, config_path, [
                       'id', 'name', 'salary'], ['id'], catalog=catalog)

        # Delete 2 employees
        delete_data = b'1,Simba Kwach,25000\n3,Da Capo,3000'
        delete_from_table(
            delete_data, config_path,
            ['id', 'name', 'salary'], ['id'], catalog=catalog)

        tbl = catalog.load_table(TABLE_NAME)
        arrow_table = tbl.scan().to_arrow()

        assert arrow_table.num_rows == 1

        arrow_table = tbl.scan(row_filter='id=2').to_arrow()
        assert arrow_table.num_rows == 1


def test_update():
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse_path = os.path.join(tmpdir, WAREHOUSE_PATH)
        os.makedirs(warehouse_path)

        catalog = get_catalog(warehouse_path)

        create_table(NAMESPACE, TABLE_NAME, catalog)

        config_path = os.path.join(tmpdir, 'config')

        with open(config_path, 'w') as f:
            f.write(DEFAULT_CONFIG_DATA)

        insert_data = \
            b'1,Simba Kwach,25000\n2,Masiza Omota,2309\n3,Da Capo,3000'
        write_to_table(insert_data, config_path, [
                       'id', 'name', 'salary'], ['id'], catalog=catalog)

        # Update salary to 5000 for 2 employees
        update_data = b'1,Simba Kwach,5000\n3,Da Capo,5000'
        write_to_table(
            update_data, config_path,
            ['id', 'name', 'salary'], ['id'], catalog=catalog)

        tbl = catalog.load_table(TABLE_NAME)
        arrow_table = tbl.scan().to_arrow()

        assert arrow_table.num_rows == 3

        arrow_table = tbl.scan(row_filter='salary=5000').to_arrow()
        assert arrow_table.num_rows == 2
