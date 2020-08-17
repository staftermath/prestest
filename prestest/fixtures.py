import pytest
from pathlib import Path

from prestest.container import Container, CONTAINER_NAMES, LOCAL_FILE_STORE_NODE

DOCKER_FOLDER = Path(".").resolve().parent / "docker-hive"


@pytest.fixture()
def container(request) -> Container:
    mark = request.node.get_closest_marker("container_folder")
    container_folder = DOCKER_FOLDER if mark is None else mark.args[0]
    return Container(docker_folder=container_folder)


@pytest.fixture()
def start_container(request, container):
    mark = request.node.get_closest_marker("enable_table_modification")
    reset = request.node.get_closest_marker("reset")
    if mark is not None:
        container.reset(allow_table_modification=True, autostart=True, until_started=True)
    elif reset:
        container.reset(until_started=True)
    else:
        container.start(until_started=True)
