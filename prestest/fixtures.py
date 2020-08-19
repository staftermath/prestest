import pytest
from pathlib import Path

from .container import Container, CONTAINER_NAMES, LOCAL_FILE_STORE_NODE
from .utils import get_prestest_params

DOCKER_FOLDER = Path(".").resolve().parent / "docker-hive"


@pytest.fixture()
def container(request) -> Container:
    container_folder = get_prestest_params(request, "container_folder", DOCKER_FOLDER)
    return Container(docker_folder=container_folder)


@pytest.fixture()
def start_container(request, container):
    allow_table_modification = get_prestest_params(request, "allow_table_modification", False)
    reset = get_prestest_params(request, "reset", False)
    if reset:
        container.reset(allow_table_modification=allow_table_modification, autostart=True, until_started=True)
    else:
        container.start(until_started=True)
