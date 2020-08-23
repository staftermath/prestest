.. _fixtures:

Fixture Parameters
==================

Fixtures need to be imported together to avoid missing depdencies.

.. code-block:: python

    from prestest.fixtures import *

Some fixtures can be configured at runtime by passing certain argument through :code:`@pytest.mark.prestest`.

.. _fixture_container:

container
---------
- **Scope**: "function"
- **Functinality**: a Container class containing methods to operate containers used in prestest
- **Dependencies**: None
- **Example**

  .. code-block:: python

    @pytest.mark.prestest(container_folder="Your docker hive folder")

container_folder
    + **Type**: PosixPath or str
    + **Required**: No
    + **Default**: './docker-hive'
    + **Functionality**: docker hive repository folder location. It must be cloned from
      `docker-hive <https://github.com/big-data-europe/docker-hive>`_.    +

.. _fixture_db_manager:

db_manager
----------
- **Scope**: "function"
- **Functinality**: a DBManager class containing methods to run presto queries.
- **Dependencies**: None
- **Example**

  .. code-block:: python

    @pytest.mark.prestest(container_folder="Your docker hive folder")

container_folder
    + **Type**: PosixPath or str
    + **Required**: No
    + **Default**: './docker-hive'
    + **Functionality**: docker hive repository folder location. It must be cloned from
      `docker-hive <https://github.com/big-data-europe/docker-hive>`_.


start_container
---------------
- **Scope**: "function"
- **Functionality**: start containers.
- **Dependencies**: :ref:`container <fixture_container>`
- **Example**

  .. code-block:: python

    @pytest.mark.prestest(allow_table_modification=True, reset=True, container_folder="Your docker hive folder")


container_folder
    + **Type**: PosixPath or str
    + **Required**: No
    + **Default**: './docker-hive'
    + **Functionality**: docker hive repository folder location. It must be cloned from
      `docker-hive <https://github.com/big-data-europe/docker-hive>`_.

allow_table_modification
    + **Type**: boolean
    + **Required**: No
    + **Default**: False
    + **Functionality**: Whether presto connector is allowed to modify hive tables. This modifies `hive.properties` file
      inside presto connector container. It automatically restart the container. This may take up to 40 seconds

reset
    + **Type**: boolean
    + **Required**: No
    + **Default**: False
    + **Functionality**: Whether containers are reset to factory states. This can help purge all changes you have made
      to the containers.


create_temporary_table
----------------------
- **Scope**: "function"
- **Functionality**: create temporary hive/presto tables and clean up after test.
- **Dependencies**: :ref:`fixture_db_manager`, :ref:`fixture_container`
- **Example**

  .. code-block:: python

        @pytest.mark.prestest(table_name="sandbox.test_temp_table", query=create_temporary_table_query,
                              file=resource_folder / "sample_table.csv")


table_name:
    + **Type**: str
    + **Required**: yes
    + **Functionality**: name of the table to be created. For example, "sandbox.test_table"

query:
    + **Type**: str
    + **Required**: yes
    + **Functionality**: query used to create the target table specified by `table_name`. This needs to have a python
      string placeholder 'table_name'. For example,

      .. code-block:: python

          table_name = """CREATE TABLE {table_name} (
                col1 INTEGER,
                col2 STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            """
file:
    + **Type**: PosixPath or str
    + **Required**: yes
    + **Functionality**: path to the file to be inserted into the created table

Fixtures
========

.. automodule:: prestest.fixtures
    :members:
    :undoc-members:
    :show-inheritance:
