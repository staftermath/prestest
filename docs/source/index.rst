.. prestest documentation master file, created by
   sphinx-quickstart on Sun Aug 23 10:59:05 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to prestest's documentation!
====================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   container
   db
   fixtures
   utils

Introduction
============

A Python library providing a testing environment for hive table creation and cleaning through presto connector. It lets
you mock the environment where you need to execute presto query and operate on tables managed by hive.

Installation
============

The latest version can be install from checking master branch at `github <https://github.com/staftermath/prestest>`_ and
install with the following command:

.. code-block:: bash

  python setup.py install

You also need to `install docker daemon <https://docs.docker.com/get-docker/>`_. You may check if it is installed by:

.. code-block:: bash

  docker ps

This library is heavily dependent on `hive docker <https://hub.docker.com/r/bde2020/hive/>`_. You need to clone hive
docker file repo from `docker-hive github <https://github.com/big-data-europe/docker-hive>`_. In this doc, we will refer
the folder containing cloned repo as **docker folder**. For example, `/tmp/repos/docker-hive`. If prestest's repos is
cloned recursively, you may find the docker folder under root of this repo.

An Example
===============

The simplest use case is from the `fixtures`:

.. code-block:: python

   from prestest.fixtures import *  # Do not import individually as fixtures have dependency that needs to be imported

To use in your test:

.. code-block:: python

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
