import pytest
from pathlib import Path

from .container import Container, CONTAINER_NAMES, LOCAL_FILE_STORE_NODE
from .db import DBManager
from .utils import get_prestest_params

DOCKER_FOLDER = Path(".").resolve().parent / "docker-hive"


class PrestestException(Exception):
    def __init__(self, msg):
        super(PrestestException, self).__init__(msg)


@pytest.fixture()
def container(request) -> Container:
    """a Container fixture. You may pass "container_folder" argument in pytest.mark.prestest. By default, it uses
    """
    container_folder = get_prestest_params(request, "container_folder", DOCKER_FOLDER)
    return Container(docker_folder=container_folder)


@pytest.fixture()
def start_container(request, container):
    """Start hive container with presto connector. You may pass the following args in pytest.mark.prestest:

    - allow_table_modification: enable table to be dropped from presto client
    - reset: completely wipe containers before starting. This will reset the containers to factory state.
    """
    allow_table_modification = get_prestest_params(request, "allow_table_modification", False)
    reset = get_prestest_params(request, "reset", False)
    if reset:
        container.reset(allow_table_modification=allow_table_modification, autostart=True, until_started=True)
    else:
        container.start(until_started=True)


@pytest.fixture()
def db_manager(request):
    """return a DBManager object using specified container. You may pass the location of hive docker in
    pytest.mark.prestest in "container_folder" argument.
    """
    container_folder = get_prestest_params(request, "container_folder", DOCKER_FOLDER)
    return DBManager(docker_folder=container_folder)


@pytest.fixture()
def create_temporary_table(request, db_manager, container) -> str:
    """create temporary table in container that gets cleaned up after test. You may pass the following param in
    pytest.mark.prestest:

    - table_name: string. name of the table, for example: sandbox.test_table
    - query: string. hive query used to create the table. You may have a string placeholder: table_name in it.
    - file: string or PosixPath. path to local file used to insert to the temporary file

    :return: created table name
    """
    table_name = get_prestest_params(request, "table_name", None)
    query = get_prestest_params(request, "query", None)
    file = get_prestest_params(request, "file", None)
    if table_name is None or query is None or file is None:
        raise PrestestException("table_name or query or file is missing from closest mark")

    query = query.format(table_name = table_name)
    db_manager.create_table(table=table_name, query=query, file=Path(file))
    yield table_name
    db_manager.drop_table(table_name)
