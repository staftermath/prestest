from pathlib import Path
import pytest
from pandas.testing import assert_frame_equal
import pandas as pd
from sqlalchemy.exc import DatabaseError

from prestest.db import DBManager
from tests.test_container import container, start_container, DOCKER_FOLDER

resource_folder = Path(".").resolve() / "resources"


@pytest.fixture()
def db_manager(start_container):
    return DBManager(docker_folder=DOCKER_FOLDER)


def test_db_manager_create_drop_table_and_read_correctly(db_manager):
    table_name = "test_db.test_table"
    query = f"""CREATE TABLE {table_name} (
        col1 INTEGER,
        col2 STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','    
    STORED AS TEXTFILE
    """
    table_name = "test_db.test_table"
    db_manager.create_table(table=table_name, query=query, file=resource_folder/"sample_table.csv")

    select_temp_table_query = f"""SELECT * FROM {table_name}"""
    result = db_manager.read_sql(select_temp_table_query)
    expected = pd.DataFrame({"col1": [123, 456], "col2": ["abc", "cba"]})
    assert_frame_equal(result, expected)

    # test table drop
    db_manager.drop_table(table=table_name)

    with pytest.raises(DatabaseError):
        db_manager.read_sql(select_temp_table_query)
