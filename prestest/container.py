"""implement functions to build, start, stop and clean up containers
"""
import subprocess
from pathlib import Path, PosixPath
from typing import Union
import docker

import logging

CONTAINER_NAMES = {
    "hive-metastore": "docker-hive_hive-metastore_1",
    "datanode": "docker-hive_datanode_1",
    "namenode": "docker-hive_namenode_1",
    "hive-server": "docker-hive_hive-server_1",
    "presto_coordinator": "docker-hive_presto-coordinator_1",
    "hive-metastore-postgresql": "docker-hive_hive-metastore-postgresql_1"
}

class Container:
    def __init__(self, docker_folder: Union[PosixPath, str]):
        self.docker_folder = Path(docker_folder).resolve()
        self.client = docker.from_env()

    def start(self):
        command = "docker-compose up -d"
        process = subprocess.Popen(command, cwd=self.docker_folder, shell=True, stdout=subprocess.PIPE)
        process.wait()

    def stop(self):
        command = f"docker-compose stop"
        process = subprocess.Popen(command, cwd=self.docker_folder, shell=True, stdout=subprocess.PIPE)
        process.wait()

    def is_started(self):
        for component, name in CONTAINER_NAMES.items():
            container = self.client.containers.get(name)
            if container.status != "running":
                logging.debug(f"[{component}] {name} is not running")
                return False

        return True
