import pytest
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
from tests.test_container import container, start_container
from tests.test_db import db_manager
from prestest.spark import spark

resource_folder = Path(".").resolve() / "resources"


def test_spark_initiate_correctly(spark):
    result = spark.sql("SELECT 1 as col").toPandas()
    expected = pd.DataFrame({"col": pd.Series([1], dtype="int32")})
    assert_frame_equal(result, expected)


@pytest.fixture()
def setup_test_db(request, db_manager, container, start_container):
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
    db_manager.create_table(table=table_name, query=query, file=resource_folder / "sample_table.csv")

    def fin():
        db_manager.drop_table(table_name)

    request.addfinalizer(fin)
    return table_name

def test_spark_connect_to_hive_container_correctly(spark, setup_test_db, db_manager):
    result = spark.sql(f"SELECT * FROM {setup_test_db}").toPandas()
    expected = pd.DataFrame({"col1": pd.Series([123, 456], dtype="int32"), "col2": ["abc", "cba"]})
    assert_frame_equal(result, expected)
