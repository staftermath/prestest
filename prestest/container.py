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
from docker.errors import NotFound

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

    def start(self, until_started=True):
        """start docker containers.

        :return: None
        """
        command = "docker-compose up -d"
        process = subprocess.Popen(command, cwd=self.docker_folder, shell=True, stdout=subprocess.PIPE)
        process.wait()
        if until_started:
            maximum_wait = 40
            sleep = 3
            while maximum_wait >= 0 and not self.is_healthy():
                time.sleep(sleep)
                maximum_wait -= sleep

            if maximum_wait < 0 and not self.is_healthy():
                raise RuntimeError("docker is not started in time")


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

    def reset(self, allow_table_modification=False, autostart=False, until_started=False):
        """remove created container. This will clear all data and metastore and restore the container to factory state.

        :param allow_table_modification: reset and allow presto connector to modify hive tables
        :param autostart: restart container after reset
        :param until_started: wait until completed restarted. if True, container will be force autostarted.
        :return: None
        """
        self.stop()
        # start reset container
        for name, container in CONTAINER_NAMES.items():
            try:
                container = self.client.containers.get(container)
                logging.debug(f"removing container {container} ({container.id})")
                self.api_client.remove_container(container.id)
            except NotFound:
                continue

        if until_started:
            # force autostart if requested to complete start
            autostart = True

        if allow_table_modification:
            self.start()
            self.enable_table_modification()
            # table access require docker restart
            self.stop()

        if autostart:
            self.start(until_started)

    def copy_from_local(self, from_local: Union[PosixPath, str], to_container: Union[PosixPath, str],
                        container_name: str=CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]):
        """copy folder or file from host to container

        :param from_local: target folder or file to be copied.
        :param to_container: container path where the folder or file will be copied to.
        :param container_name: name of the container containing the target file `to_container`
        :return: None
        """
        command = f"docker cp {from_local} {container_name}:{to_container}"
        self.execute_command(command)

    def download_from_container(self, from_container, to_local,
                                container_name: str=CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]):
        """download target folder or file to host

        :param from_container: target folder or file to be downloaded to host.
        :param to_local: location on host.
        :param container_name: name of the container containing the target file `from_container`
        :return: None
        """
        command = f"docker cp {container_name}:{from_container} {to_local}"
        self.execute_command(command)

    def delete(self, target, container_name: str=CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]):
        """call rm -rf command on `target` inside datanode container

        :param target: target folder or file
        :param container_name: name of the container containing the target file
        :return: None
        """
        command = f"docker exec {container_name} rm -rf {target}"
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
        stdout, stderr = res[0].decode('ascii'), res[1].decode('ascii')
        if stderr != '' or 'permission denied' in stdout.lower():
            raise exception(f"STDOUT: {stdout}. STDERR: {stderr}")
        return stdout

    def upload_temp_table_file(self, local_file, container_name: str=CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]):
        """return a context manager to upload the file to a temp file in container. See `TempContainerFile` for details

        :example:
        >>> with self.upload_temp_table_file('local_file', 'target_container') as f:
        ...     # do something with f where f is the temporary file name in the container.

        """
        return TempContainerFile(self, local_file, container_name)

    def append_file(self, container_name, file, text, skip_if_exists=True, user='root', from_new_line=True):
        """append a line to target `file` in target `container_name`. You can choose to skip (by default) if the text
        already exists in target file. you may change user in the docker by specifying `user`. If `from_new_line` is
        True, the text will always be appended in a new line. otherwise directly appended at EOF.

        :param container_name: name of the container the target file is contained in.
        :param file: target file in the container.
        :param text: content to append.
        :param skip_if_exists: if text already exists in file, do not append duplicate. the checking will strip white
          space.
        :param user: user used to execute the operation. e.g. 'root', '1000'
        :param from_new_line: if True, the text will always be appended from a new line.
        :return: None
        """
        if skip_if_exists:
            command_check_if_exists = f"""docker exec -u 1000 -t {container_name} grep "{text.strip()}" {file}"""
            result = self.execute_command(command_check_if_exists)
            if result != '':
                return

        line_break = "\n" if from_new_line else ""
        command_append = f"""docker exec -u {user} -t {container_name} bash -c 'echo \"{line_break}{text}\" >> {file}'"""
        self.execute_command(command_append)

    def enable_table_modification(self):
        """edit hive.properties file in presto connector server so that hive table can be modified in presto.
        note that this may fail if presto server version changes

        :return: None
        """
        properties = [
            "hive.allow-drop-table=true",
            "hive.allow-rename-table=true",
            "hive.allow-add-column=true"
        ]
        catalog_path = "/opt/presto-server-0.181/etc/catalog/hive.properties"
        for property in properties:
            self.append_file(container_name=CONTAINER_NAMES["presto_coordinator"],
                             file=catalog_path,
                             text=property,
                             user='root',
                             from_new_line=False)


class TempContainerFile:
    """a context manager class used to upload file to temporary file in target container. You need to pass a Container
    object and target `container_name`. The temporary file will be removed at exit. Note that the temporary file in
    target container will have the same permission as `local_file`.
    """
    def __init__(self, container: Container, local_file: Union[PosixPath, str],
                 container_name: str=CONTAINER_NAMES[LOCAL_FILE_STORE_NODE]):
        self.container = container
        self.container_name = container_name
        self.local_file = local_file
        self.temp_file = None

    def __enter__(self):
        self.temp_file = Path("/tmp") / str(uuid.uuid4())
        self.container.copy_from_local(self.local_file, self.temp_file, self.container_name)
        return self.temp_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.delete(self.temp_file, self.container_name)
