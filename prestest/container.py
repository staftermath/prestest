"""implement functions to build, start, stop and clean up containers
"""
import subprocess
from pathlib import Path, PosixPath
from typing import Union
import logging
import time

import pandas as pd
from sqlalchemy import create_engine
import docker


CONTAINER_NAMES = {
    "hive-metastore": "docker-hive_hive-metastore_1",
    "datanode": "docker-hive_datanode_1",
    "namenode": "docker-hive_namenode_1",
    "hive-server": "docker-hive_hive-server_1",
    "presto_coordinator": "docker-hive_presto-coordinator_1",
    "hive-metastore-postgresql": "docker-hive_hive-metastore-postgresql_1"
}


class Container:
    """contains method to control and examine hive/presto container used for test
    """
    def __init__(self, docker_folder: Union[PosixPath, str]):
        self.docker_folder = Path(docker_folder).resolve()
        self.client = docker.from_env()
        self.api_client = docker.APIClient()

    def start(self):
        """start docker containers.

        :return: None
        """
        command = "docker-compose up -d"
        process = subprocess.Popen(command, cwd=self.docker_folder, shell=True, stdout=subprocess.PIPE)
        process.wait()

    def stop(self):
        """stop containers

        :return:
        """
        command = f"docker-compose stop"
        process = subprocess.Popen(command, cwd=self.docker_folder, shell=True, stdout=subprocess.PIPE)
        process.wait()

    def is_started(self) -> bool:
        """check if container has properly started.

        :return:
        """
        for component, name in CONTAINER_NAMES.items():
            container = self.client.containers.get(name)
            if container.status != "running":
                logging.debug(f"[{component}] {name} is not running")
                return False

        return True

    def is_healthy(self) -> bool:
        """check if container is healthy. if exited containers are contained not healthy. if health condition is not
        configured, the container is considered healthy. A healthy container is read to take request such as presto
        queries.

        :return: whether container is healthy or not.
        """
        if not self.is_started():
            return False

        for component, name in CONTAINER_NAMES.items():
            # Get health info. if container not up. return empty dict

            state = self.api_client.inspect_container(name)["State"]
            if "Health" in state:
                status = state["Health"]["Status"]
                if status != "healthy":
                    logging.debug(f"[{component} {name} is not healthy. got {status}")
                    return False

        return True

    def is_presto_started(self) -> bool:
        """examine if presto server has properly started. This will try 5 times before determining that the server
        cannot be connected. It will sleep 5 seconds between retries.

        :return: whether presto server is started.
        """
        presto_client = create_engine("presto://localhost:8080", connect_args={"protocol": "http"})

        attempts = 5
        sleep = 5
        while attempts > 0:
            try:
                with presto_client.connect() as con:
                    pd.read_sql("SELECT * FROM system.runtime.nodes LIMIT 1", con=con)
                    return True
            except:
                attempts -= 1
                time.sleep(sleep)

        return False
