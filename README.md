# prestest: A Test Framework for Presto connector on Hive

In many applications, when we develop libraries or ETL processes, we intend to deploy on an environment with Hive and 
presto connections. Usually presto is connect to hive. It could be difficult to develop a unit test framework to 
test the functionality such as creating table using hive and then access them through presto. This library 
provides a module to help build such test framework for your code. 

## Installation

Install with the following command:
``` bash
python setup.py install
```

You also need to <a href=https://docs.docker.com/get-docker/>install docker daemon</a>. You may check if it is installed by:
``` bash
docker ps
```

This library is heavily dependent on <a href=https://hub.docker.com/r/bde2020/hive/>hive docker</a>. You need to clone 
hive docker file repo from <a href=https://github.com/big-data-europe/docker-hive>docker-hive github</a>. In this doc, 
we will refer the folder containing cloned repo as **docker folder**. For example, `/tmp/repos/docker-hive`. If 
prestest's repos is cloned recursively, you may find the docker folder under root of this repo. The following container 
names are default containers used in docker-hive. They should not be taken in your docker daemon:

- docker-hive_hive-metastore_1
- docker-hive_datanode_1
- docker-hive_namenode_1
- docker-hive_hive-server_1
- docker-hive_presto-coordinator_1
- docker-hive_hive-metastore-postgresql_1

The library contains some functions that may
 
- start all containers above.
- stop all containers above.
- reset all containers above to factory.

The test helpers are implemented under pytest. However, you may uses the modules and develop in any other test 
frameworks.

## Example

The simplest use case is from the `fixtures`:

``` python
from prestest.fixtures import *  # Do not import individually as fixtures have dependency that needs to be imported
```

To use in your test:

``` python
from pandas.testing import assert_frame_equal
import pandas as pd
from sqlalchemy.exc import DatabaseError

docker_folder = 'Your docker_folder'
@pytest.mark.prestest(allow_table_modification=True, reset=True,
                  container_folder=docker_folder)
def test_start_container_enable_table_modification_allow_presto_table_creation_and_drop(
       start_container, db_manager):
   # create a sample table through presto
   table_name = "sandbox.test_table"
   create_table = f"""
   CREATE TABLE IF NOT EXISTS {table_name} AS
   SELECT
       1 AS col1,
       'dummy' AS col2
   """
   db_manager.read_sql(create_table)

   # test that the table is created properly and can be queried from presto
   select_table = f"SELECT * FROM {table_name}"
   result = db_manager.read_sql(select_table)
   expected = pd.DataFrame({"col1": [1], "col2": ["dummy"]})
   assert_frame_equal(result, expected)

   # test table dropping is permitted
   db_manager.read_sql(f"DROP TABLE {table_name}")
   # test that the table is properly dropped
   with pytest.raises(DatabaseError):
       db_manager.read_sql(select_table)
```

Some fixtures can be configured through `@pytext.mark.prestest`. See `fixtures` for details.
