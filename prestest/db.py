"""implement interface to create and clean up tables
"""
from pathlib import PosixPath
from typing import Union

import pandas as pd
from sqlalchemy import create_engine
from .container import PRESTO_URL, Container

class DBManager:
    """implement method to create, remove tables in testing framework.
    """
    def __init__(self, docker_folder):
        self.hive_client = self.get_hive_client()
        self.presto_client = self.get_presto_client()
        self.container = Container(docker_folder)

    def get_hive_client(self):
        return create_engine("hive://localhost:10000")

    def get_presto_client(self):
        return create_engine(PRESTO_URL, connect_args={"protocol": "http"})

    def create_table(self, table: str, query: str, file: Union[PosixPath, str]):
        """create table based on the query and insert file into the table. this method intends to help set up tables
        used for testing. the database for the table will be created (but not dropped after)

        :param table: name of the table. for example, 'sandbox.my_table'
        :param query: a query used to create hive table.
        :param file: a file inserted to the table. This will overwrite the table if it already exists.
        :return: None
        """
        schema, _ = table.split(".")
        create_db = f"""CREATE DATABASE IF NOT EXISTS {schema}"""
        self.hive_client.execute(create_db)
        self.drop_table(table)

        with self.container.upload_temp_table_file(local_file=file) as filename:
            self.hive_client.execute(query)
            insert_to_table = f"""LOAD DATA LOCAL INPATH '{filename}' OVERWRITE INTO TABLE {table}"""
            self.hive_client.execute(insert_to_table)

    def drop_table(self, table:str):
        """drop target table in container hive.

        :param table: name of the table.
        :return: None
        """
        drop_table = f"""DROP TABLE IF EXISTS {table}"""
        self.hive_client.execute(drop_table)

    def read_sql(self, query: str) -> pd.DataFrame:
        """download presto query result into a pandas dataframe.

        :param query: a presto query.
        :return: a dataframe containing the returned contents of the query.
        """
        with self.presto_client.connect() as con:
            df = pd.read_sql(query, con=con)
        return df
