import pytest
from pathlib import Path

from pandas.testing import assert_frame_equal
import pandas as pd
from sqlalchemy.exc import DatabaseError

from prestest.fixtures import container, start_container, db_manager, create_temporary_table
from prestest.container import CONTAINER_NAMES

resource_folder = Path(".").resolve() / "resources"


@pytest.mark.prestest(container_folder=resource_folder)
def test_container_set_docker_folder_correctly(container):
    assert container.docker_folder == resource_folder


@pytest.mark.prestest(container_folder=resource_folder)
def test_db_manager_set_docker_folder_correctly(db_manager):
    assert db_manager.container.docker_folder == resource_folder


@pytest.mark.prestest(reset=True)
def test_start_container_disable_table_modification_do_not_change_hive_properties(start_container, container, tmpdir):
    temp_download = Path(tmpdir.join("test_start_container_enable_table_modification_hive_properties"))
    container.download_from_container(from_container="/opt/presto-server-0.181/etc/catalog/hive.properties",
                                      to_local=temp_download,
                                      container_name=CONTAINER_NAMES["presto_coordinator"])
    with open(temp_download, 'r') as f:
        result = set(l.strip() for l in f.readlines() if l.strip() != '')
    expected = {"hive.allow-drop-table=true", "hive.allow-rename-table=true", "hive.allow-add-column=true"}
    assert not result.intersection(expected)


@pytest.mark.prestest(allow_table_modification=True, reset=True)
def test_start_container_enable_table_modification_correctly(start_container, container, tmpdir):
    temp_download = Path(tmpdir.join("test_start_container_enable_table_modification_hive_properties"))
    container.download_from_container(from_container="/opt/presto-server-0.181/etc/catalog/hive.properties",
                                      to_local=temp_download,
                                      container_name=CONTAINER_NAMES["presto_coordinator"])
    with open(temp_download, 'r') as f:
        result = set(l.strip() for l in f.readlines() if l.strip() != '')
    expected = {"hive.allow-drop-table=true", "hive.allow-rename-table=true", "hive.allow-add-column=true"}
    assert result.issuperset(expected)


@pytest.mark.prestest(reset=True)
def test_start_container_reset_correctly(start_container, container, tmpdir):
    temp_download = Path(tmpdir.join("test_start_container_enable_table_modification_hive_properties"))
    container.download_from_container(from_container="/opt/presto-server-0.181/etc/catalog/hive.properties",
                                      to_local=temp_download,
                                      container_name=CONTAINER_NAMES["presto_coordinator"])
    with open(temp_download, 'r') as f:
        result = set(l.strip() for l in f.readlines() if l.strip() != '')
    expected = {"hive.allow-drop-table=true", "hive.allow-rename-table=true", "hive.allow-add-column=true"}
    assert not result.intersection(expected)


@pytest.fixture()
def clean_up_table(db_manager):
    table_name = "sandbox.test_table"
    db_manager.drop_table(table=table_name)
    db_manager.run_hive_query(f"CREATE DATABASE IF NOT EXISTS sandbox")
    yield table_name
    db_manager.drop_table(table=table_name)
    db_manager.run_hive_query(f"DROP DATABASE IF EXISTS sandbox")


@pytest.mark.prestest(allow_table_modification=True, reset=True)
def test_start_container_enable_table_modification_allow_presto_table_creation_and_drop(
        start_container, db_manager, clean_up_table):
    table_name = clean_up_table
    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} AS
    SELECT 
        1 AS col1,
        'dummy' AS col2
    """
    db_manager.read_sql(create_table)

    select_table = f"SELECT * FROM {table_name}"
    result = db_manager.read_sql(select_table)
    expected = pd.DataFrame({"col1": [1], "col2": ["dummy"]})
    assert_frame_equal(result, expected)

    db_manager.read_sql(f"DROP TABLE {table_name}")
    with pytest.raises(DatabaseError):
        db_manager.read_sql(select_table)


create_temporary_table_query = """CREATE TABLE {table_name} (
    col1 INTEGER,
    col2 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','    
STORED AS TEXTFILE
"""


@pytest.mark.prestest(table_name="sandbox.test_temp_table", query=create_temporary_table_query,
                      file=resource_folder / "sample_table.csv")
def test_create_temporary_table_create_table_correctly(create_temporary_table, db_manager):
    result = db_manager.read_sql("SELECT * FROM sandbox.test_temp_table")
    expected = pd.DataFrame({"col1": [123, 456], "col2": ["abc", "cba"]})
    assert_frame_equal(result, expected)
