"""implement functions to build, start, stop and clean up containers
"""
import subprocess
from pathlib import Path, PosixPath
from typing import Union
import logging
import time
import uuid

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

LOCAL_FILE_STORE_NODE = "hive-server"

PRESTO_URL = "presto://localhost:8080"


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
        presto_client = create_engine(PRESTO_URL, connect_args={"protocol": "http"})

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

    def reset(self):
        """remove created container. This will clear all data and metastore and restore the container to factory state.

        :return: None
        """
        self.stop()
        for name, container in CONTAINER_NAMES.items():
            container = self.client.containers.get(container)
            logging.debug(f"removing container {container} ({container.id})")
            self.api_client.remove_container(container.id)

    def copy_from_local(self, from_local: Union[PosixPath, str], to_container: Union[PosixPath, str]):
        """copy folder or file from host to container

        :param from_local: target folder or file to be copied.
        :param to_container: container path where the folder or file will be copied to.
        :return: None
        """
        datanode_name = CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]
        command = f"docker cp {from_local} {datanode_name}:{to_container}"
        self.execute_command(command)

    def download_from_container(self, from_container, to_local):
        """download target folder or file to host

        :param from_container: target folder or file to be downloaded to host.
        :param to_local: location on host.
        :return: None
        """
        datanode_name = CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]
        command = f"docker cp {datanode_name}:{from_container} {to_local}"
        self.execute_command(command)

    def delete(self, target):
        """call rm -rf command on `target` inside datanode container

        :param target: target folder or file
        :return: None
        """
        datanode_name = CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]
        command = f"docker exec {datanode_name} rm -rf {target}"
        self.execute_command(command)

    def execute_command(self, command, exception=RuntimeError):
        """execute a command as subprocess, raise specific type of exception if any error occurs.

        :param command: a string of terminal command
        :param exception: type of exception raised when any error happends.
        :return: None
        """
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        res = process.communicate()
        error = res[1].decode('ascii')
        if error != '':
            raise exception(error)

    def upload_temp_table_file(self, local_file):
        return TempContainerFile(self, local_file)


class TempContainerFile:

    def __init__(self, container: Container, local_file: Union[PosixPath, str]):
        self.container = container
        self.local_file = local_file
        self.temp_file = None

    def __enter__(self):
        self.temp_file = Path("/tmp") / str(uuid.uuid4())
        self.container.copy_from_local(self.local_file, self.temp_file)
        return self.temp_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.delete(self.temp_file)
